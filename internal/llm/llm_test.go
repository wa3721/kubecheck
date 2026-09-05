package llm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newFakeLLMServer 启动 OpenAI 兼容的 fake 服务：验证请求（model/key/批量 JSON），
// 按配置返回判定结果；返回服务地址与请求记录。
func newFakeLLMServer(t *testing.T, statusCode int, contentFn func(lines []string) string) (url string, requests *[][]string) {
	t.Helper()
	seen := &[][]string{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer test-key" {
			t.Errorf("Authorization = %q, want Bearer test-key", got)
		}
		var body struct {
			Model       string `json:"model"`
			Stream      bool   `json:"stream"`
			Temperature int    `json:"temperature"`
			Messages    []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		if body.Model != "test-model" {
			t.Errorf("model = %q, want test-model", body.Model)
		}
		if body.Stream {
			t.Errorf("stream must be false (非流式)")
		}
		if body.Temperature != 0 {
			t.Errorf("temperature = %d, want 0", body.Temperature)
		}
		var lines []string
		if len(body.Messages) == 2 && body.Messages[1].Role == "user" {
			if err := json.Unmarshal([]byte(body.Messages[1].Content), &lines); err != nil {
				t.Errorf("user content 非合法 JSON 数组: %v", err)
			}
		}
		*seen = append(*seen, lines)

		w.WriteHeader(statusCode)
		if contentFn != nil {
			respBody := map[string]interface{}{
				"choices": []map[string]interface{}{
					{"message": map[string]string{"content": contentFn(lines)}},
				},
			}
			_ = json.NewEncoder(w).Encode(respBody)
		}
	}))
	t.Cleanup(srv.Close)
	return srv.URL, seen
}

func TestEnabled(t *testing.T) {
	if New("", "m", "k", time.Second).Enabled() {
		t.Fatal("空 endpoint 应禁用")
	}
	if !New(DefaultEndpoint, DefaultModel, DefaultAPIKey, 0).Enabled() {
		t.Fatal("默认参数应启用")
	}
	var nilClient *Client
	if nilClient.Enabled() {
		t.Fatal("nil 客户端应禁用")
	}
}

func TestDefaults(t *testing.T) {
	if DefaultEndpoint == "" || DefaultModel == "" || DefaultAPIKey == "" {
		t.Fatal("默认 endpoint/model/api-key 不能为空")
	}
	c := New("", "", "", 0)
	if c.Timeout() != DefaultTimeoutSec*time.Second {
		t.Fatalf("默认超时 = %v", c.Timeout())
	}
}

func TestJudgeSuccess(t *testing.T) {
	url, seen := newFakeLLMServer(t, http.StatusOK, func(lines []string) string {
		results := make([]string, 0, len(lines))
		for _, l := range lines {
			if strings.Contains(l, "fatal") {
				results = append(results, `{"is_error":true}`)
			} else {
				results = append(results, `{"is_error":false}`)
			}
		}
		return `{"results":[` + strings.Join(results, ",") + `]}`
	})
	c := New(url, "test-model", "test-key", 5*time.Second)

	res, err := c.Judge(context.Background(), []string{"fatal: db down", "normal info", "fatal: lost conn"})
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	want := []bool{true, false, true}
	if len(res) != len(want) {
		t.Fatalf("结果长度 = %d, want %d", len(res), len(want))
	}
	for i := range want {
		if res[i] != want[i] {
			t.Fatalf("结果[%d] = %v, want %v", i, res[i], want[i])
		}
	}
	if len(*seen) != 1 || len((*seen)[0]) != 3 {
		t.Fatalf("请求次数/行数异常: %v", *seen)
	}
}

func TestJudgeEmptyInput(t *testing.T) {
	c := New("http://x", "m", "k", time.Second)
	res, err := c.Judge(context.Background(), nil)
	if err != nil || res != nil {
		t.Fatalf("空输入应返回 (nil, nil)，got (%v, %v)", res, err)
	}
}

// TestJudgeBatchSplit 25 行超出单批上限 20 -> 拆分为 2 次请求，结果顺序正确
func TestJudgeBatchSplit(t *testing.T) {
	url, seen := newFakeLLMServer(t, http.StatusOK, func(lines []string) string {
		results := make([]string, 0, len(lines))
		for range lines {
			results = append(results, `{"is_error":true}`)
		}
		return `{"results":[` + strings.Join(results, ",") + `]}`
	})
	c := New(url, "test-model", "test-key", 5*time.Second)

	lines := make([]string, 25)
	for i := range lines {
		lines[i] = "error line"
	}
	res, err := c.Judge(context.Background(), lines)
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if len(res) != 25 {
		t.Fatalf("结果长度 = %d, want 25", len(res))
	}
	for i, v := range res {
		if !v {
			t.Fatalf("结果[%d] 应为 true", i)
		}
	}
	if len(*seen) != 2 || len((*seen)[0]) != 20 || len((*seen)[1]) != 5 {
		t.Fatalf("批次拆分异常: %v", *seen)
	}
}

// TestJudgeMarkdownFence 模型返回带 ```json 围栏的内容 -> 仍能正确解析
func TestJudgeMarkdownFence(t *testing.T) {
	url, _ := newFakeLLMServer(t, http.StatusOK, func(lines []string) string {
		return "```json\n{\"results\":[{\"is_error\":true}]}\n```"
	})
	c := New(url, "test-model", "test-key", 5*time.Second)
	res, err := c.Judge(context.Background(), []string{"boom"})
	if err != nil {
		t.Fatalf("Judge: %v", err)
	}
	if len(res) != 1 || !res[0] {
		t.Fatalf("围栏内容解析失败: %v", res)
	}
}

func TestJudgeHTTPError(t *testing.T) {
	url, _ := newFakeLLMServer(t, http.StatusInternalServerError, nil)
	c := New(url, "test-model", "test-key", 5*time.Second)
	if _, err := c.Judge(context.Background(), []string{"error"}); err == nil {
		t.Fatal("HTTP 500 应返回错误")
	}
}

func TestJudgeLengthMismatch(t *testing.T) {
	url, _ := newFakeLLMServer(t, http.StatusOK, func(lines []string) string {
		return `{"results":[{"is_error":true}]}`
	})
	c := New(url, "test-model", "test-key", 5*time.Second)
	if _, err := c.Judge(context.Background(), []string{"a", "b"}); err == nil {
		t.Fatal("结果数与输入行数不一致应返回错误")
	}
}

func TestJudgeGarbageContent(t *testing.T) {
	url, _ := newFakeLLMServer(t, http.StatusOK, func(lines []string) string {
		return "这不是 JSON"
	})
	c := New(url, "test-model", "test-key", 5*time.Second)
	if _, err := c.Judge(context.Background(), []string{"a"}); err == nil {
		t.Fatal("非法 JSON 内容应返回错误")
	}
}

func TestJudgeTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	c := New(srv.URL, "test-model", "test-key", 200*time.Millisecond)
	start := time.Now()
	if _, err := c.Judge(context.Background(), []string{"error"}); err == nil {
		t.Fatal("超时应返回错误")
	}
	if time.Since(start) > time.Second {
		t.Fatalf("超时未生效，耗时 %v", time.Since(start))
	}
}
