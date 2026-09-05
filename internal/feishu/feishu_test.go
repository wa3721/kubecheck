package feishu

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestHeaderTemplate(t *testing.T) {
	if headerTemplate(EventPodStatus) != "red" {
		t.Fatal("abnormal event should be red")
	}
	if headerTemplate(EventPendingCheck) != "orange" {
		t.Fatal("pending-check event should be orange")
	}
}

func TestSignFeishu(t *testing.T) {
	f := New("https://hook", "secret", 0)
	ts := "1700000000"
	sign := f.signFeishu(ts)
	// 校验签名算法：base64(HMAC-SHA256(secret, ts+"\n"+secret))
	raw, err := base64.StdEncoding.DecodeString(sign)
	if err != nil {
		t.Fatalf("sign not base64: %v", err)
	}
	if len(raw) != 32 { // SHA256 = 32 bytes
		t.Fatalf("sign length wrong: %d", len(raw))
	}
}

func TestEnabled(t *testing.T) {
	if New("", "", 0).Enabled() {
		t.Fatal("empty webhook should be disabled")
	}
	if !New("https://hook", "", 0).Enabled() {
		t.Fatal("non-empty webhook should be enabled")
	}
}

func TestBuildPayload(t *testing.T) {
	f := New("https://hook", "secret", 0)
	payload, err := f.buildPayload(EventPodStatus, "myapp", "detail here")
	if err != nil {
		t.Fatal(err)
	}
	var msg struct {
		MsgType string `json:"msg_type"`
		Card    struct {
			Header struct {
				Template string `json:"template"`
				Title    struct {
					Content string `json:"content"`
				} `json:"title"`
			} `json:"header"`
		} `json:"card"`
		Timestamp string `json:"timestamp"`
		Sign      string `json:"sign"`
	}
	if err := json.Unmarshal(payload, &msg); err != nil {
		t.Fatal(err)
	}
	if msg.MsgType != "interactive" {
		t.Fatalf("msg_type = %q", msg.MsgType)
	}
	if msg.Card.Header.Template != "red" {
		t.Fatalf("template = %q", msg.Card.Header.Template)
	}
	if msg.Timestamp == "" {
		t.Fatal("timestamp not set")
	}
	if msg.Sign == "" {
		t.Fatal("sign missing")
	}
	// 卡片正文必须包含资源名（key）与详情，与控制台降级输出三要素一致
	raw := string(payload)
	if !strings.Contains(raw, "myapp") {
		t.Fatalf("card body missing resource name (key): %s", raw)
	}
	if !strings.Contains(raw, "detail here") {
		t.Fatalf("card body missing detail: %s", raw)
	}

	// 无 secret 时不应带 sign/timestamp
	f2 := New("https://hook", "", 0)
	p2, _ := f2.buildPayload(EventPodStatus, "myapp", "d")
	var m2 map[string]interface{}
	_ = json.Unmarshal(p2, &m2)
	if _, ok := m2["sign"]; ok {
		t.Fatal("sign should be absent when secret empty")
	}
}

func TestDedup(t *testing.T) {
	f := New("https://hook", "", 10*time.Second)
	if f.dedup("k") {
		t.Fatal("first send should not be deduped")
	}
	if !f.dedup("k") {
		t.Fatal("second send within window should be deduped")
	}
}

// TestSend 通过 httptest 验证发送逻辑（成功路径）
func TestSend(t *testing.T) {
	var gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, `{"code":0,"msg":"success"}`)
	}))
	defer srv.Close()

	f := New(srv.URL, "", 0)
	f.Send(EventPodStatus, "myapp", "detail")
	// 非阻塞发送，短暂等待 goroutine 完成
	time.Sleep(200 * time.Millisecond)
	if gotBody == "" {
		t.Fatal("no request received by test server")
	}
	if !strings.Contains(gotBody, "interactive") {
		t.Fatalf("payload missing interactive card: %s", gotBody)
	}
	// 卡片正文应包含资源名（key）与详情
	if !strings.Contains(gotBody, "myapp") || !strings.Contains(gotBody, "detail") {
		t.Fatalf("card body should contain key and detail: %s", gotBody)
	}
}

// TestSendDisabled 未启用时不发送
func TestSendDisabled(t *testing.T) {
	f := New("", "", 0)
	// 不应 panic 或阻塞
	f.Send(EventPodStatus, "myapp", "detail")
}

// TestSendDisabledConsoleFallback 未配置 webhook 时降级控制台：
// 输出含事件类型/资源名/详情三要素（与卡片正文一致），且去重窗口内不重复打印
func TestSendDisabledConsoleFallback(t *testing.T) {
	f := New("", "", 10*time.Second)

	output := captureStdout(t)
	f.Send(EventPodStatus, "myapp", "detail here")
	f.Send(EventPodStatus, "myapp", "detail here") // 窗口内重复 -> 去重
	out := output()

	for _, want := range []string{"[ALERT]", EventPodStatus, "myapp", "detail here"} {
		if !strings.Contains(out, want) {
			t.Fatalf("console fallback missing %q, got: %q", want, out)
		}
	}
	if strings.Count(out, "[ALERT]") != 1 {
		t.Fatalf("duplicate alert should be deduped, got %d alerts: %q", strings.Count(out, "[ALERT]"), out)
	}
}

// TestSendDisabledConsoleFallbackPendingCheck 提示类事件（日志需检查，Pod 存活）：
// 控制台级别为 [WARN]（黄色）而非 [ALERT]（红色）——飞书卡片橙色与退出码 0 不受影响
func TestSendDisabledConsoleFallbackPendingCheck(t *testing.T) {
	f := New("", "", 10*time.Second)

	output := captureStdout(t)
	f.Send(EventPendingCheck, "myapp", "detail here")
	out := output()

	for _, want := range []string{"[WARN]", EventPendingCheck, "myapp", "detail here"} {
		if !strings.Contains(out, want) {
			t.Fatalf("console fallback missing %q, got: %q", want, out)
		}
	}
	if strings.Contains(out, "[ALERT]") {
		t.Fatalf("pending-check event should print [WARN] not [ALERT], got: %q", out)
	}
}

// captureStdout 重定向标准输出，返回恢复函数与捕获内容
func captureStdout(t *testing.T) func() string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	return func() string {
		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stdout = old
		return buf.String()
	}
}
