package feishu

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"kubecheck/internal/console"
)

// 事件类型（用于消息标题、header 着色与去重 key）
const (
	EventPodStatus     = "Pod状态异常"
	EventRestartLimit  = "重启次数超限"
	EventNoTargetPod   = "未找到目标Pod"
	EventPendingCheck  = "日志需检查"
	EventContainerExit = "容器异常退出"
	EventInterrupted   = "检查被中断"
)

// FeishuAlert 飞书自定义机器人告警客户端（非阻塞发送）
type FeishuAlert struct {
	webhook  string
	secret   string
	dedupWin time.Duration
	client   *http.Client

	mu       sync.Mutex
	lastSent map[string]time.Time
}

// New 创建告警客户端；webhook 为空则告警不可用
func New(webhook, secret string, dedupWin time.Duration) *FeishuAlert {
	if dedupWin <= 0 {
		dedupWin = 30 * time.Second
	}
	return &FeishuAlert{
		webhook:  webhook,
		secret:   secret,
		dedupWin: dedupWin,
		client:   &http.Client{Timeout: 3 * time.Second},
		lastSent: make(map[string]time.Time),
	}
}

// Enabled 告警是否启用
func (f *FeishuAlert) Enabled() bool {
	return f != nil && f.webhook != ""
}

// Send 非阻塞发送：去重通过后进入独立 goroutine 推送，失败仅记录 warning。
// 未配置飞书 Webhook 时降级为控制台直接打印（同样做去重，避免刷屏）。
func (f *FeishuAlert) Send(eventType, title, detail string) {
	if !f.Enabled() {
		f.printConsole(eventType, title, detail)
		return
	}
	if f.dedup(eventType + "|" + title) {
		return
	}
	go f.post(eventType, title, detail)
}

// printConsole 控制台告警输出（未配置 Webhook 或发送失败的统一降级格式，带去重）。
// 格式与飞书卡片正文一致：事件类型 + 资源名（title）+ 详情（detail）三要素齐全。
// 级别与着色：异常类事件 [ALERT] 红色；提示类（EventPendingCheck：日志命中真错误但
// Pod 存活）[WARN] 黄色——飞书卡片仍为橙色、退出码仍为 0，不受控制台级别影响。
func (f *FeishuAlert) printConsole(eventType, title, detail string) {
	if f.dedup(eventType + "|" + title) {
		return
	}
	level, color := "[ALERT]", console.Red
	if eventType == EventPendingCheck {
		level, color = "[WARN]", console.Yellow
	}
	fmt.Printf("\n%s\n----------------------------\n",
		color(fmt.Sprintf("%s[%s] %s\n%s", level, eventType, title, detail)))
}

// dedup 同 Pod + 同事件在去重窗口内仅发送一条
func (f *FeishuAlert) dedup(key string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := time.Now()
	if t, ok := f.lastSent[key]; ok && now.Sub(t) < f.dedupWin {
		return true
	}
	f.lastSent[key] = now
	return false
}

// post 实际发送（独立 goroutine 执行）。
// 统一降级逻辑：http 调用发生网络错误或状态码非 200 时，把告警内容打印到控制台，不 panic。
func (f *FeishuAlert) post(eventType, title, detail string) {
	payload, err := f.buildPayload(eventType, title, detail)
	if err != nil {
		fmt.Println(console.Yellow(fmt.Sprintf("[WARN] 构造飞书消息失败: %v", err)))
		f.printConsole(eventType, title, detail)
		return
	}
	resp, err := f.client.Post(f.webhook, "application/json", bytes.NewReader(payload))
	if err != nil {
		fmt.Println(console.Yellow(fmt.Sprintf("[WARN] 飞书告警发送失败: %v", err)))
		// 降级：告警内容打印到控制台，保证告警不丢失
		f.printConsole(eventType, title, detail)
		return
	}
	defer resp.Body.Close()

	var r struct {
		Code int    `json:"code"`
		Msg  string `json:"msg"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&r)
	if resp.StatusCode != http.StatusOK || r.Code != 0 {
		fmt.Println(console.Yellow(fmt.Sprintf("[WARN] 飞书告警返回异常: status=%d code=%d msg=%s", resp.StatusCode, r.Code, r.Msg)))
		// 降级：告警内容打印到控制台
		f.printConsole(eventType, title, detail)
	}
}

// buildPayload 构造 interactive 消息卡片（header 按事件类型着色）。
// 卡片正文与控制台降级输出同为三要素：资源名（title）+ 详情（detail），
// 控制台以事件类型前缀呈现，卡片以 header 标题呈现。
func (f *FeishuAlert) buildPayload(eventType, title, detail string) ([]byte, error) {
	card := map[string]interface{}{
		"config": map[string]interface{}{"wide_screen_mode": true},
		"header": map[string]interface{}{
			"template": headerTemplate(eventType),
			"title": map[string]interface{}{
				"tag":     "plain_text",
				"content": fmt.Sprintf("kubectl-check · %s", eventType),
			},
		},
		"elements": []interface{}{
			map[string]interface{}{
				"tag":  "div",
				"text": map[string]interface{}{"tag": "lark_md", "content": fmt.Sprintf("**资源**: %s", title)},
			},
			map[string]interface{}{
				"tag":  "div",
				"text": map[string]interface{}{"tag": "lark_md", "content": detail},
			},
			map[string]interface{}{"tag": "hr"},
			map[string]interface{}{
				"tag": "note",
				"elements": []interface{}{
					map[string]interface{}{
						"tag":     "plain_text",
						"content": "kubectl-check 监控告警 · " + time.Now().Format("2006-01-02 15:04:05"),
					},
				},
			},
		},
	}

	msg := map[string]interface{}{
		"msg_type": "interactive",
		"card":     card,
	}
	if f.secret != "" {
		ts := fmt.Sprintf("%d", time.Now().Unix())
		msg["timestamp"] = ts
		msg["sign"] = f.signFeishu(ts)
	}
	return json.Marshal(msg)
}

// headerTemplate 按事件类型着色：提示类 orange，异常类 red
func headerTemplate(eventType string) string {
	switch eventType {
	case EventPendingCheck:
		return "orange"
	default:
		return "red"
	}
}

// signFeishu 飞书签名：HMAC-SHA256(secret, timestamp+"\n"+secret) 后 base64
func (f *FeishuAlert) signFeishu(timestamp string) string {
	s := fmt.Sprintf("%s\n%s", timestamp, f.secret)
	mac := hmac.New(sha256.New, []byte(f.secret))
	mac.Write([]byte(s))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
