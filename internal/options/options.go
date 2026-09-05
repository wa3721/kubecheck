package options

import "time"

// Options 汇总 kubectl-check 全部命令行参数（与 SPEC 参数清单对应）
// 时间相关参数统一为秒级整数输入，内部转换为 time.Duration。
type Options struct {
	Namespace    string
	ResourceType string
	ResourceName string

	AllNamespaces bool // -A/--all-namespaces：监听所有命名空间的 Deployment，创建或更新时自动检查（常驻模式）
	WatchMode     bool // 监听模式入口（-A，或无位置参数且 -n 显式给定）：由 parseArgs 判定，run 分流到 watcher
	// 监听模式的命名空间过滤（逗号分隔，支持 * / ? 通配，如 *-prod 匹配所有 prod 后缀；nil 表示全部）
	WatchNamespaces []string

	// 两段独立超时（秒级），彼此互不影响，无整体超时控制
	PodReadyTimeoutSec int // 第一级：Pod 就绪（Ready）超时
	LogCheckTimeoutSec int // 第二级：日志跟踪超时

	MaxRestart     int
	CheckPodStatus bool
	LogEnable      bool

	LogErrKeywords    []string
	LogIgnoreKeywords []string
	LogTail           int
	LogErrorDir       string
	LogDump           bool // 错误日志落盘开关：默认 false 不落盘，开启后命中真错误/容器异常退出时写文件
	LogConsole        bool // 日志控制台输出开关：默认 true 把追踪流读到的日志行逐行实时输出到控制台（同 kubectl logs -f）；false 时静默
	KeywordCheck      bool // 错误关键字判定开关：默认 true 由 LogErrKeywords 决定；false 时 parseArgs 清空关键字列表（errorHit 恒 false；关键字为 LLM 仲裁唯一入口，关闭后 LLM 无输入）

	// LLM 日志错误仲裁（OpenAI 兼容协议，默认智谱 GLM）：
	// 命中错误关键字的日志行由 LLM 异步判定真伪，存在真错误才全量落盘+告警。
	// endpoint 非空即启用（默认值非空 -> 默认启用）；显式置空 endpoint 才禁用，
	// 禁用后回退"关键字即真"的现行行为。调用失败降级为真错误（宁可误报不漏报）。
	LLMEndpoint   string
	LLMModel      string
	LLMApiKey     string
	LLMTimeoutSec int
	LLMEnable     bool // LLM 仲裁开关：默认 true 由 LLMEndpoint 决定；false 时 parseArgs 清空端点（回退关键字即真，等效 --llm-endpoint=""） // LLM 单次判定超时（秒）

	FeishuWebhook        string
	FeishuSecret         string
	FeishuDedupWindowSec int // 同 Pod+同事件告警去重窗口（秒）

	Verbose bool
}

// PodReadyTimeout Pod 状态就绪超时时长
func (o *Options) PodReadyTimeout() time.Duration {
	return time.Duration(o.PodReadyTimeoutSec) * time.Second
}

// LogCheckTimeout 日志跟踪超时时长
func (o *Options) LogCheckTimeout() time.Duration {
	return time.Duration(o.LogCheckTimeoutSec) * time.Second
}

// FeishuDedupWindow 告警去重窗口
func (o *Options) FeishuDedupWindow() time.Duration {
	return time.Duration(o.FeishuDedupWindowSec) * time.Second
}

// LLMTimeout LLM 单次判定超时时长
func (o *Options) LLMTimeout() time.Duration {
	return time.Duration(o.LLMTimeoutSec) * time.Second
}
