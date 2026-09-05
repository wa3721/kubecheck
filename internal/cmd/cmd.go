package cmd

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"kubecheck/internal/checker"
	"kubecheck/internal/console"
	"kubecheck/internal/llm"
	"kubecheck/internal/options"
	"kubecheck/internal/watcher"
)

// NewCheckCommand 构建 kubectl-check 的 cobra 命令
func NewCheckCommand() *cobra.Command {
	opts := &options.Options{}

	cmd := &cobra.Command{
		Use:   "check deployment/<name> [flags]",
		Short: "监视 Deployment 滚动更新与 Pod 状态、日志并发送告警",
		Long: `输入 deployment 名称与 namespace，定位目标 Pod 后按两级独立超时校验 Pod 层级。
rollout 由 CICD 触发，本工具只监视与告警，不做任何实际更新操作：

  目标定位：在 --pod-ready-timeout 窗口内等待本次发布（最新 ReplicaSet）的目标
  Pod 出现（覆盖"发布刚触发、新 RS 已创建但 Pod 尚未创建"的竞态窗口），不校验
  Deployment 的超时与滚动状态；replicas=0 无目标可等或窗口内未出现，则发送告警
  并直接退出（码 2）。

  阶段2：目标 Pod 各自独立 --pod-ready-timeout 等待就绪（Ready，含就绪探针校验；
    无探针容器 Running 即就绪）；超时置全局 pod failure（发送告警，最终退出码 3）。

  阶段3：每个 Pod 从首次 Running 时刻起独立 --log-check-timeout 观察日志（follow 增量），
    与阶段2 并行（有探针时 0/1 Running 期间两阶段共同作用；Ready 前容器崩溃立即告警，
    不等就绪超时）；窗口结束无容器崩溃则正常退出（命中真错误仅提醒，退出码 0）。
    命中错误关键字的日志行由 LLM 异步仲裁真伪：
    存在真错误 -> 置全局 warning（退出码 0）；全部假错误 -> 不告警；LLM 调用失败 ->
    降级为关键字即真。容器异常退出（退出码 != 0）不经仲裁 -> 退出码 3。
    日志内容判定默认全部关闭：--keyword-check 开启关键字命中（--log-err-keywords
    提供关键字）；--llm-enable 开启 LLM 仲裁（--llm-endpoint 提供端点，默认智谱 GLM；
    关键字为仲裁唯一入口，需配合 --keyword-check）；两者均关时不影响退出检测。
    --log-console（默认开启）把追踪流读到的日志逐行实时输出到控制台
    （效果同 kubectl logs --follow，与判定/落盘互不影响）。
    错误日志落盘默认关闭，--log-dump 开启后：命中真错误以容器启动时刻为起点全量
    拉取日志落盘、容器异常退出无条件落盘保留现场（--log-error-dir 指定目录）。

  最终：等待全部 goroutine（含日志落盘）完成后按全局状态聚合退出。
  中断（SIGINT/SIGTERM）：取消全部超时上下文、等待日志落盘、发送中断告警后非 0 退出。

  监听模式（常驻）：-A 监听全部命名空间，或 -n 指定命名空间过滤（逗号分隔多值、
  支持 * / ? 通配，如 -n '*-prod'）后不带资源参数。匹配命名空间的 Deployment
  创建或更新（generation 递增）时自动执行上述检查逻辑；启动时已存在的存量
  Deployment 不触发。`,
		Example: `  # 单次检查
  kubectl check -n delta deployment/my-app \
      --pod-ready-timeout 300 --log-check-timeout 60 \
      --log-err-keywords=error,panic --log-error-dir=./errlogs

  # 监听全部命名空间
  kubectl check -A

  # 监听 prod 后缀命名空间（Deployment 创建/更新自动检查）
  kubectl check -n '*-prod'

  # 监听指定多个命名空间
  kubectl check -n delta,gamma`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			if err := parseArgs(c, args, opts); err != nil {
				return err
			}
			return run(c, opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&opts.Namespace, "namespace", "n", "",
		"目标命名空间。单次检查模式（带资源参数）：须为精确单值，未指定时用 default；"+
			"监听模式（无资源参数）：支持逗号分隔多值与 * / ? 通配（如 *-prod 匹配所有 prod 后缀命名空间），"+
			"匹配命名空间的 Deployment 创建或更新时自动检查")
	flags.BoolVarP(&opts.AllNamespaces, "all-namespaces", "A", false,
		"监听所有命名空间的 Deployment，创建或更新时自动启动检查（常驻模式，无需指定资源）")

	flags.IntVar(&opts.PodReadyTimeoutSec, "pod-ready-timeout", 300, "第一级：Pod 就绪（Ready）超时（秒）")
	flags.IntVar(&opts.LogCheckTimeoutSec, "log-check-timeout", 60, "第二级：日志跟踪超时（秒），<=0 视为禁用")
	flags.IntVar(&opts.MaxRestart, "max-restart", 0, "Pod 最大允许重启次数")
	flags.BoolVar(&opts.CheckPodStatus, "check-pod-status", true, "Pod 状态强校验开关")
	flags.BoolVar(&opts.LogEnable, "log-enable", true, "实时日志监控开关")
	flags.StringSliceVar(&opts.LogErrKeywords, "log-err-keywords",
		[]string{"error", "panic", "fatal", "exception", "crash"}, "日志异常关键字（逗号分隔），仅 --keyword-check 开启时生效")
	flags.BoolVar(&opts.KeywordCheck, "keyword-check", false, "错误关键字判定开关（默认 false 关闭：errorHit 恒 false，场景3 提醒不触发；关键字为 LLM 仲裁唯一入口，需配合 --llm-enable）；开启后由 --log-err-keywords 决定关键字")
	flags.StringSliceVar(&opts.LogIgnoreKeywords, "log-ignore-keywords", nil, "忽略的无害日志关键字（逗号分隔）")
	flags.IntVar(&opts.LogTail, "log-tail", 100, "回溯读取日志行数")
	flags.StringVar(&opts.LogErrorDir, "log-error-dir", ".", "错误日志落盘目录（文件：namespace-podname-date.log），仅 --log-dump 开启时生效")
	flags.BoolVar(&opts.LogDump, "log-dump", false, "错误日志落盘开关：默认 false 不落盘；开启后命中真错误/容器异常退出时写文件")
	flags.BoolVar(&opts.LogConsole, "log-console", true, "日志控制台输出开关：默认 true 把追踪阶段读到的日志逐行实时输出到控制台（效果同 kubectl logs --follow，与关键字/LLM 判定及落盘互不影响）；false 静默")
	flags.StringVar(&opts.FeishuWebhook, "feishu-webhook", "", "飞书机器人 Webhook 地址，非空启用告警")
	flags.StringVar(&opts.FeishuSecret, "feishu-secret", "", "飞书签名校验密钥（HMAC-SHA256）")
	flags.IntVar(&opts.FeishuDedupWindowSec, "feishu-dedup-window", 30, "同 Pod+同事件告警去重窗口（秒）")
	flags.StringVar(&opts.LLMEndpoint, "llm-endpoint", llm.DefaultEndpoint,
		"日志错误仲裁 LLM 端点（OpenAI 兼容，默认智谱 GLM），仅 --llm-enable 开启时生效")
	flags.BoolVar(&opts.LLMEnable, "llm-enable", false, "LLM 日志仲裁开关（默认 false 关闭，回退关键字即真）；开启后由 --llm-endpoint 决定端点（需配合 --keyword-check 提供命中输入）")
	flags.StringVar(&opts.LLMModel, "llm-model", llm.DefaultModel, "日志错误仲裁模型")
	flags.StringVar(&opts.LLMApiKey, "llm-api-key", llm.DefaultAPIKey, "日志错误仲裁 API Key")
	flags.IntVar(&opts.LLMTimeoutSec, "llm-timeout", llm.DefaultTimeoutSec, "LLM 单次判定超时（秒）")
	flags.BoolVarP(&opts.Verbose, "verbose", "v", false, "详细日志输出")

	return cmd
}

// Execute 执行 cobra 命令
func Execute() {
	if err := NewCheckCommand().Execute(); err != nil {
		// cobra RunE 返回 error 统一按 code=1 处理（参数错误等）
		os.Exit(1)
	}
}

// parseArgs 解析位置参数并校验：支持 "deployment/name" 与 "deployment name" 两种 kubectl 风格。
// 模式判定：
//   - -A/--all-namespaces            -> 监听模式，全部命名空间（不可带资源参数）
//   - 无资源参数且 -n 显式给定        -> 监听模式，-n 作为命名空间过滤（多值/通配）
//   - 带资源参数                       -> 单次检查模式，-n 须为精确单值（未指定回填 default）
func parseArgs(c *cobra.Command, args []string, opts *options.Options) error {
	switch {
	case opts.AllNamespaces:
		if len(args) > 0 {
			return fmt.Errorf("-A/--all-namespaces 模式下无需指定资源（将监听所有命名空间的 Deployment 创建与更新）")
		}
		opts.WatchMode = true
		opts.WatchNamespaces = nil // 全部命名空间
	case len(args) == 0:
		// 无资源参数：仅当 -n 显式给定（非空）才进入监听模式
		if opts.Namespace == "" {
			return fmt.Errorf("缺少资源参数：指定 deployment/<名称> 进行单次检查，或 -A 监听全部命名空间，或 -n <命名空间>（支持多值/通配）进入监听模式")
		}
		opts.WatchMode = true
		opts.WatchNamespaces = strings.Split(opts.Namespace, ",")
	default:
		// 单次检查模式
		if strings.ContainsAny(opts.Namespace, ",*?") {
			return fmt.Errorf("单次检查模式下 -n 须为精确命名空间（多值/通配符仅用于监听模式，即不带资源参数）")
		}
		if opts.Namespace == "" {
			opts.Namespace = "default" // 向后兼容：未指定 -n 时默认 default
		}
		var res string
		if len(args) == 1 {
			res = args[0]
		} else {
			res = strings.Join(args, "/")
		}
		parts := strings.SplitN(res, "/", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return fmt.Errorf("资源参数格式错误，应为 <类型>/<名称>，例如 deployment/my-app")
		}
		opts.ResourceType = strings.ToLower(parts[0])
		opts.ResourceName = parts[1]
		if opts.ResourceType != "deployment" {
			return fmt.Errorf("暂不支持的资源类型: %s（当前仅支持 deployment）", opts.ResourceType)
		}
	}
	if opts.PodReadyTimeoutSec <= 0 {
		return fmt.Errorf("--pod-ready-timeout 必须大于 0（秒）")
	}
	if opts.LogCheckTimeoutSec < 0 {
		return fmt.Errorf("--log-check-timeout 不能为负数")
	}
	if opts.MaxRestart < 0 {
		return fmt.Errorf("--max-restart 不能为负数")
	}
	// bool 开关折算（优先级高于对应字符串参数；两者默认 false，即默认关闭日志内容判定）：
	//   --llm-enable=false（默认）    -> 清空端点，禁用 LLM 仲裁（回退关键字即真）
	//   --keyword-check=false（默认） -> 清空错误关键字列表，关闭关键字命中判定
	if !opts.LLMEnable {
		opts.LLMEndpoint = ""
	}
	if !opts.KeywordCheck {
		opts.LogErrKeywords = nil
	}
	if opts.LLMEndpoint != "" && opts.LLMTimeoutSec <= 0 {
		return fmt.Errorf("--llm-timeout 必须大于 0（秒）")
	}
	if opts.LogErrorDir == "" {
		opts.LogErrorDir = "."
	}
	return nil
}

// run 主流程编排：构建客户端 -> 注册信号处理 -> Checker.Run -> 按退出码退出。
// 监听模式（-A 或 -n 过滤）走 watcher 常驻流程（Deployment 创建/更新自动检查）。
func run(c *cobra.Command, opts *options.Options) error {
	clientset, err := buildClient()
	if err != nil {
		fmt.Println(console.Red(fmt.Sprintf("[ERROR] 构建 Kubernetes 客户端失败: %v", err)))
		return err
	}

	if opts.WatchMode {
		return runWatchMode(opts, clientset)
	}

	chk := checker.New(opts, clientset)

	// 信号处理：注册 SIGINT、SIGTERM 处理器。
	// 收到中断信号：调用所有 cancel 函数（全部 cancel_pod_ready、全部 cancel_pod_log），
	// wg.Wait() 等待正在写日志的 goroutine 完成文件落盘，
	// 组装中断告警发送（飞书 / 降级控制台），非 0 退出。
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		chk.Interrupt()
		os.Exit(checker.CodeInterrupted)
	}()

	code := chk.Run(context.Background())
	os.Exit(code)
	return nil
}

// runWatchMode 常驻监听模式（-A 全部命名空间，或 -n 过滤）：注册信号处理
// （中断时停止监听并中断全部进行中的检查），然后启动 watcher
// 监听 Deployment 的创建与更新（generation 递增；初始存量不触发）。
func runWatchMode(opts *options.Options, clientset kubernetes.Interface) error {
	w := watcher.New(opts, clientset, opts.WatchNamespaces)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println(console.Green("[INFO] 收到中断信号，停止监听并中断所有进行中的检查"))
		cancel() // 停止 informer 监听
		w.Interrupt()
		os.Exit(checker.CodeInterrupted)
	}()

	code := w.Run(ctx)
	os.Exit(code)
	return nil
}

// buildClient 构建 K8s 客户端（优先 kubeconfig，支持 KUBECONFIG 环境变量）
func buildClient() (*kubernetes.Clientset, error) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	configOverrides := &clientcmd.ConfigOverrides{}
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, configOverrides).ClientConfig()
	if err != nil {
		return nil, err
	}
	return kubernetes.NewForConfig(config)
}
