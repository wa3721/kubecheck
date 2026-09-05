// Package llm 提供 OpenAI 兼容协议的日志错误仲裁客户端（默认智谱 GLM）。
//
// 用途：阶段3 日志检查中，命中错误关键字的日志行是否为"真实报错"由 LLM 仲裁，
// 只有存在真错误才触发全量日志落盘与告警，降低关键字误报。
// 全部用标准库 net/http + encoding/json 实现，不引入额外依赖。
//
// 注意：日志行会发送至配置的 endpoint（默认为云端服务），敏感日志场景应
// 指向内网自建的兼容端点；api key 仅用于请求头，不会出现在日志输出中。
package llm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// 默认参数：命令行未指定时使用（智谱 GLM，OpenAI 兼容协议）。
// DefaultAPIKey 内置于源码：仓库若公开需注意泄露风险，可通过 --llm-api-key 覆盖并轮换。
const (
	DefaultEndpoint   = "https://open.bigmodel.cn/api/paas/v4/chat/completions"
	DefaultModel      = "GLM-4-Flash-250414"
	DefaultAPIKey     = "d8c44eb30f4a4ec4a2db213aa480448c.W0hKnAJ7jeBoYoRN"
	DefaultTimeoutSec = 15
)

// judgeSystemPrompt 批量判定提示词（在"单行判定只返回 {"is_error":bool}"的原义上
// 扩展为多行批量格式，一次调用判定多行以控制请求次数）。
const judgeSystemPrompt = `你是容器日志判断专家，能判断用户输入的日志内容是否是真实的报错日志。
用户输入为 JSON 字符串数组（每个元素是一条日志行）。请逐行判断每条日志是否为真实的报错，
只允许返回 JSON：{"results":[{"is_error":true},{"is_error":false},...]}，
results 数组长度必须与输入行数一致，不允许返回其他任何内容。`

// maxBatchLines 单次判定请求的最大行数（超出拆分为多次调用，控制请求与响应规模）
const maxBatchLines = 20

// Client OpenAI 兼容 chat completions 客户端
type Client struct {
	endpoint string
	model    string
	apiKey   string
	timeout  time.Duration
	hc       *http.Client
}

// New 构造客户端；timeout <= 0 时使用 DefaultTimeoutSec。
// endpoint 为空串时 Enabled() 返回 false（禁用仲裁，调用方回退"关键字即真"）。
func New(endpoint, model, apiKey string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = DefaultTimeoutSec * time.Second
	}
	return &Client{
		endpoint: strings.TrimSpace(endpoint),
		model:    strings.TrimSpace(model),
		apiKey:   strings.TrimSpace(apiKey),
		timeout:  timeout,
		hc:       &http.Client{},
	}
}

// Enabled endpoint 非空即启用（默认参数非空 -> 默认启用；显式置空 endpoint 才禁用）
func (c *Client) Enabled() bool { return c != nil && c.endpoint != "" }

// Timeout 单次判定的超时时长（供调用方计算在途判定的等待上限）
func (c *Client) Timeout() time.Duration {
	if c == nil {
		return 0
	}
	return c.timeout
}

// Judge 批量判定日志行是否为真实错误，返回与 lines 等长的布尔切片。
// 网络/超时/响应解析失败返回 error，由调用方降级为"关键字即真"。
func (c *Client) Judge(ctx context.Context, lines []string) ([]bool, error) {
	if len(lines) == 0 {
		return nil, nil
	}
	results := make([]bool, 0, len(lines))
	for start := 0; start < len(lines); start += maxBatchLines {
		end := start + maxBatchLines
		if end > len(lines) {
			end = len(lines)
		}
		bs, err := c.judgeBatch(ctx, lines[start:end])
		if err != nil {
			return nil, err
		}
		results = append(results, bs...)
	}
	return results, nil
}

// judgeBatch 单次请求判定一批日志行（非流式：不带 stream，直接解析完整 JSON 响应）
func (c *Client) judgeBatch(ctx context.Context, lines []string) ([]bool, error) {
	userContent, err := json.Marshal(lines)
	if err != nil {
		return nil, err
	}
	reqBody := map[string]interface{}{
		"model": c.model,
		"messages": []map[string]string{
			{"role": "system", "content": judgeSystemPrompt},
			{"role": "user", "content": string(userContent)},
		},
		"temperature": 0, // 判定任务确定性优先
		"stream":      false,
	}
	payload, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	cctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(cctx, http.MethodPost, c.endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.apiKey)

	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("llm endpoint 返回 %d: %s", resp.StatusCode, truncate(string(body), 200))
	}

	var cr chatResponse
	if err := json.Unmarshal(body, &cr); err != nil {
		return nil, fmt.Errorf("解析响应失败: %w", err)
	}
	if len(cr.Choices) == 0 {
		return nil, fmt.Errorf("响应缺少 choices")
	}
	results, err := parseJudgeContent(cr.Choices[0].Message.Content)
	if err != nil {
		return nil, err
	}
	if len(results) != len(lines) {
		return nil, fmt.Errorf("判定结果数量 %d 与输入行数 %d 不一致", len(results), len(lines))
	}
	return results, nil
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
}

type judgeResults struct {
	Results []struct {
		IsError bool `json:"is_error"`
	} `json:"results"`
}

// parseJudgeContent 解析模型返回的判定 JSON。
// 容忍 ```json ... ``` 代码围栏与前后杂散文本：截取首个 '{' 到最后一个 '}' 之间解析。
func parseJudgeContent(content string) ([]bool, error) {
	content = strings.TrimSpace(content)
	if i := strings.Index(content, "{"); i >= 0 {
		if j := strings.LastIndex(content, "}"); j > i {
			content = content[i : j+1]
		}
	}
	var jr judgeResults
	if err := json.Unmarshal([]byte(content), &jr); err != nil {
		return nil, fmt.Errorf("解析判定结果 %q 失败: %w", truncate(content, 100), err)
	}
	out := make([]bool, len(jr.Results))
	for i, r := range jr.Results {
		out[i] = r.IsError
	}
	return out, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
