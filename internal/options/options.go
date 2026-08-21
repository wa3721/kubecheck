package options

import "time"

// Options 汇总 kubectl-check 全部命令行参数（与 SPEC 参数清单对应）
// 时间相关参数统一为秒级整数输入，内部转换为 time.Duration。
type Options struct {
	Namespace    string
	ResourceType string
	ResourceName string

	// 三段独立超时（秒级），彼此互不影响，无整体超时控制
	DeployReadyTimeoutSec int // 第一层：Deployment 就绪超时
	PodReadyTimeoutSec    int // 第二层：Pod 状态就绪超时
	LogCheckTimeoutSec    int // 第二层：日志跟踪超时

	MaxRestart     int
	CheckPodStatus bool
	LogEnable      bool

	LogErrKeywords    []string
	LogIgnoreKeywords []string
	LogTail           int
	LogErrorDir       string

	FeishuWebhook        string
	FeishuSecret         string
	FeishuDedupWindowSec int // 同 Pod+同事件告警去重窗口（秒）

	Verbose bool
}

// DeployReadyTimeout Deployment 就绪超时时长
func (o *Options) DeployReadyTimeout() time.Duration {
	return time.Duration(o.DeployReadyTimeoutSec) * time.Second
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
