package feishu

import (
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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
}

// TestSendDisabled 未启用时不发送
func TestSendDisabled(t *testing.T) {
	f := New("", "", 0)
	// 不应 panic 或阻塞
	f.Send(EventPodStatus, "myapp", "detail")
}
