package cmd

import (
	"testing"

	"github.com/spf13/cobra"

	"kubecheck/internal/llm"
	"kubecheck/internal/options"
)

func TestParseArgs(t *testing.T) {
	// parseArgs 仅做格式/范围校验，默认值由 cobra flag 提供，测试需先填默认值
	withDefaults := func() *options.Options {
		return &options.Options{
			PodReadyTimeoutSec: 120,
			LogCheckTimeoutSec: 60,
			LogErrorDir:        ".",
		}
	}
	c := &cobra.Command{}

	// deployment/name 形式
	opts := withDefaults()
	if err := parseArgs(c, []string{"deployment/myapp"}, opts); err != nil {
		t.Fatalf("parseArgs err: %v", err)
	}
	if opts.ResourceType != "deployment" || opts.ResourceName != "myapp" {
		t.Fatalf("got %s/%s", opts.ResourceType, opts.ResourceName)
	}

	// deployment name 形式
	opts2 := withDefaults()
	if err := parseArgs(c, []string{"deployment", "myapp2"}, opts2); err != nil {
		t.Fatalf("parseArgs (two args) err: %v", err)
	}
	if opts2.ResourceType != "deployment" || opts2.ResourceName != "myapp2" {
		t.Fatalf("got %s/%s", opts2.ResourceType, opts2.ResourceName)
	}

	// 不支持的资源类型
	opts3 := withDefaults()
	if err := parseArgs(c, []string{"statefulset/myapp"}, opts3); err == nil {
		t.Fatal("should reject non-deployment resource")
	}

	// 格式错误
	opts4 := withDefaults()
	if err := parseArgs(c, []string{"myapp"}, opts4); err == nil {
		t.Fatal("should reject malformed resource arg")
	}
}

func TestParseArgsTimeout(t *testing.T) {
	c := &cobra.Command{}
	// pod-ready-timeout<=0 应被拒绝
	opts := &options.Options{PodReadyTimeoutSec: 0, LogCheckTimeoutSec: 60, LogErrorDir: "."}
	if err := parseArgs(c, []string{"deployment/myapp"}, opts); err == nil {
		t.Fatal("pod-ready-timeout<=0 should be rejected")
	}
	// 合法配置应通过
	opts.PodReadyTimeoutSec = 120
	if err := parseArgs(c, []string{"deployment/myapp"}, opts); err != nil {
		t.Fatalf("valid config should pass: %v", err)
	}
	// log-check-timeout 允许为 0（禁用），但不可为负
	opts.LogCheckTimeoutSec = -1
	if err := parseArgs(c, []string{"deployment/myapp"}, opts); err == nil {
		t.Fatal("log-check-timeout<0 should be rejected")
	}
	opts.LogCheckTimeoutSec = 0
	if err := parseArgs(c, []string{"deployment/myapp"}, opts); err != nil {
		t.Fatalf("zero log-check-timeout should pass: %v", err)
	}
}

func TestParseArgsBoolSwitches(t *testing.T) {
	c := &cobra.Command{}

	// 默认（bool 均为 false）：LLM 仲裁与关键字判定关闭——
	// 端点被清空（llm-timeout 校验随之跳过）、错误关键字列表被清空
	opts := &options.Options{
		PodReadyTimeoutSec: 120,
		LogCheckTimeoutSec: 60,
		LogErrorDir:        ".",
		LLMEndpoint:        "https://llm.example/v1", // 即使非空也被默认 false 清空
		LLMTimeoutSec:      0,                        // 端点被清空后不再校验
		LogErrKeywords:     []string{"error", "panic"},
	}
	if err := parseArgs(c, []string{"deployment/myapp"}, opts); err != nil {
		t.Fatalf("default (both disabled) should pass: %v", err)
	}
	if opts.LLMEndpoint != "" {
		t.Fatalf("default llm-enable=false should clear endpoint, got %q", opts.LLMEndpoint)
	}
	if len(opts.LogErrKeywords) != 0 {
		t.Fatalf("default keyword-check=false should clear err keywords, got %v", opts.LogErrKeywords)
	}

	// 显式开启：字符串参数原样保留（端点/关键字生效）
	opts2 := &options.Options{
		PodReadyTimeoutSec: 120,
		LogCheckTimeoutSec: 60,
		LogErrorDir:        ".",
		KeywordCheck:       true,
		LogErrKeywords:     []string{"error", "panic"},
		LLMEnable:          true,
		LLMEndpoint:        "https://llm.example/v1",
		LLMTimeoutSec:      15,
	}
	if err := parseArgs(c, []string{"deployment/myapp"}, opts2); err != nil {
		t.Fatalf("explicit enable should pass: %v", err)
	}
	if opts2.LLMEndpoint != "https://llm.example/v1" || len(opts2.LogErrKeywords) != 2 {
		t.Fatal("explicit enable must preserve string params")
	}
}

// TestParseArgsWatchMode 监听模式判定：
//   - -A（无位置参数）-> WatchMode，WatchNamespaces=nil（全部）
//   - 无位置参数 + -n 显式给定 -> WatchMode，按逗号拆分（支持 glob）
//   - 无位置参数 + 无 -n -> 报错
//   - -A + 位置参数 -> 报错
//   - 位置参数 + -n 含通配/逗号 -> 报错；-n 未给 -> 回填 default
func TestParseArgsWatchMode(t *testing.T) {
	c := &cobra.Command{}

	// -A -> 监听模式（全部命名空间）
	opts := &options.Options{AllNamespaces: true, PodReadyTimeoutSec: 120, LogCheckTimeoutSec: 60, LogErrorDir: "."}
	if err := parseArgs(c, nil, opts); err != nil {
		t.Fatalf("-A should pass: %v", err)
	}
	if !opts.WatchMode || len(opts.WatchNamespaces) != 0 {
		t.Fatalf("-A -> WatchMode=true, WatchNamespaces=nil; got mode=%v ns=%v", opts.WatchMode, opts.WatchNamespaces)
	}
	// -A + 位置参数 -> 报错
	if err := parseArgs(c, []string{"deployment/x"}, opts); err == nil {
		t.Fatal("-A with resource arg should fail")
	}

	// 无位置参数 + -n '*-prod' -> 监听模式，glob 过滤
	opts2 := &options.Options{Namespace: "*-prod", PodReadyTimeoutSec: 120, LogCheckTimeoutSec: 60, LogErrorDir: "."}
	if err := parseArgs(c, nil, opts2); err != nil {
		t.Fatalf("-n glob should pass: %v", err)
	}
	if !opts2.WatchMode || len(opts2.WatchNamespaces) != 1 || opts2.WatchNamespaces[0] != "*-prod" {
		t.Fatalf("got mode=%v ns=%v", opts2.WatchMode, opts2.WatchNamespaces)
	}

	// 无位置参数 + -n 多值 -> 按逗号拆分
	opts3 := &options.Options{Namespace: "ns-a,ns-b", PodReadyTimeoutSec: 120, LogCheckTimeoutSec: 60, LogErrorDir: "."}
	if err := parseArgs(c, nil, opts3); err != nil {
		t.Fatalf("-n multi should pass: %v", err)
	}
	if !opts3.WatchMode || len(opts3.WatchNamespaces) != 2 {
		t.Fatalf("got mode=%v ns=%v", opts3.WatchMode, opts3.WatchNamespaces)
	}

	// 无位置参数 + 无 -n -> 报错
	opts4 := &options.Options{PodReadyTimeoutSec: 120, LogCheckTimeoutSec: 60, LogErrorDir: "."}
	if err := parseArgs(c, nil, opts4); err == nil {
		t.Fatal("no args and no -n should fail")
	}

	// 位置参数 + -n 含通配符/逗号 -> 报错（单次检查须精确命名空间）
	opts5 := &options.Options{Namespace: "*-prod", PodReadyTimeoutSec: 120, LogCheckTimeoutSec: 60, LogErrorDir: "."}
	if err := parseArgs(c, []string{"deployment/x"}, opts5); err == nil {
		t.Fatal("resource arg with glob -n should fail")
	}
	opts5.Namespace = "a,b"
	if err := parseArgs(c, []string{"deployment/x"}, opts5); err == nil {
		t.Fatal("resource arg with multi -n should fail")
	}

	// 位置参数 + 未指定 -n -> 回填 default（向后兼容）
	opts6 := &options.Options{PodReadyTimeoutSec: 120, LogCheckTimeoutSec: 60, LogErrorDir: "."}
	if err := parseArgs(c, []string{"deployment/myapp"}, opts6); err != nil {
		t.Fatalf("default ns backfill should pass: %v", err)
	}
	if opts6.Namespace != "default" || opts6.WatchMode {
		t.Fatalf("got ns=%q watch=%v, want default/false", opts6.Namespace, opts6.WatchMode)
	}
}

func TestNewCheckCommand(t *testing.T) {
	cmd := NewCheckCommand()
	if cmd.Use == "" {
		t.Fatal("command Use empty")
	}
	flags := cmd.Flags()
	// -n 默认为空：单次检查模式 parseArgs 回填 default；监听模式由显式 -n 决定过滤范围
	if ns, _ := flags.GetString("namespace"); ns != "" {
		t.Fatalf("default namespace = %q, want empty (backfilled to default in single mode)", ns)
	}
	prt, _ := flags.GetInt("pod-ready-timeout")
	if prt != 300 {
		t.Fatalf("default pod-ready-timeout = %d, want 300", prt)
	}
	lct, _ := flags.GetInt("log-check-timeout")
	if lct != 60 {
		t.Fatalf("default log-check-timeout = %d, want 60", lct)
	}
	// LLM 仲裁默认启用（智谱 GLM 默认值）
	ep, _ := flags.GetString("llm-endpoint")
	if ep != llm.DefaultEndpoint {
		t.Fatalf("default llm-endpoint = %q, want %q", ep, llm.DefaultEndpoint)
	}
	model, _ := flags.GetString("llm-model")
	if model != llm.DefaultModel {
		t.Fatalf("default llm-model = %q, want %q", model, llm.DefaultModel)
	}
	key, _ := flags.GetString("llm-api-key")
	if key != llm.DefaultAPIKey {
		t.Fatalf("default llm-api-key mismatch")
	}
	lt, _ := flags.GetInt("llm-timeout")
	if lt != llm.DefaultTimeoutSec {
		t.Fatalf("default llm-timeout = %d, want %d", lt, llm.DefaultTimeoutSec)
	}
	// bool 开关默认值（false：LLM 仲裁与关键字判定默认关闭）
	if le, _ := flags.GetBool("llm-enable"); le {
		t.Fatal("default llm-enable should be false")
	}
	if kc, _ := flags.GetBool("keyword-check"); kc {
		t.Fatal("default keyword-check should be false")
	}
	// 日志控制台输出默认开启（同 kubectl logs -f 效果）
	if lc, _ := flags.GetBool("log-console"); !lc {
		t.Fatal("default log-console should be true")
	}
}
