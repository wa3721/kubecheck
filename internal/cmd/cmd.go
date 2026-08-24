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
	"kubecheck/internal/options"
	"kubecheck/internal/watcher"
)

// NewCheckCommand 构建 kubectl-check 的 cobra 命令
func NewCheckCommand() *cobra.Command {
	opts := &options.Options{}

	cmd := &cobra.Command{
		Use:   "check deployment/<name> [flags]",
		Short: "监视 Deployment 滚动更新与 Pod 状态、日志并发送告警",
		Long: `输入 deployment 名称与 namespace，按三阶段独立超时监视。
rollout restart 由 CICD 触发，本工具只监视与告警，不做任何实际更新操作：

  阶段1（前置阻断）：用 informer 监听 Deployment 滚动更新完成
    （observedGeneration >= generation，status.replicas / updatedReplicas / readyReplicas
    均达到 spec.replicas，即旧副本已缩容且新副本全部就绪）。在 --deploy-ready-timeout
    内超时则发送告警并直接退出（码 2），不再进入后续阶段——这是固有短板：
    慢启动应用会丢失 Pod 早期日志。

  阶段2：提取本次发布（new_rs）下的每个 Pod，各自独立 --pod-ready-timeout 等待进入
    Running；超时置全局 pod failure（发送告警，最终退出码 3）。

  阶段3：每个 Pod 从 Running 时刻起独立 --log-check-timeout 观察日志（follow 增量落盘）。
    命中错误关键字 -> 置全局 warning（退出码 0，提醒关注）；
    容器异常退出（退出码 != 0）-> 置全局 pod failure（发送告警，退出码 3）。

  最终：等待全部 goroutine（含日志落盘）完成后按全局状态聚合退出。
  中断（SIGINT/SIGTERM）：取消全部超时上下文、等待日志落盘、发送中断告警后非 0 退出。`,
		Example: `  kubectl check -n delta deployment/my-app \
      --deploy-ready-timeout 300 --pod-ready-timeout 120 --log-check-timeout 60 \
      --log-err-keywords=error,panic --log-error-dir=./errlogs`,
		Args: cobra.MaximumNArgs(1),
		RunE: func(c *cobra.Command, args []string) error {
			if err := parseArgs(c, args, opts); err != nil {
				return err
			}
			return run(c, opts)
		},
	}

	flags := cmd.Flags()
	flags.StringVarP(&opts.Namespace, "namespace", "n", "default", "目标命名空间")
	flags.BoolVarP(&opts.AllNamespaces, "all-namespaces", "A", false, "监听所有命名空间的 Deployment，发生更新时自动启动检查（常驻模式，无需指定资源）")

	flags.IntVar(&opts.DeployReadyTimeoutSec, "deploy-ready-timeout", 300, "第一级：Deployment 就绪超时（秒），<=0 视为禁用")
	flags.IntVar(&opts.PodReadyTimeoutSec, "pod-ready-timeout", 120, "第二级：Pod 状态就绪超时（秒）")
	flags.IntVar(&opts.LogCheckTimeoutSec, "log-check-timeout", 60, "第二级：日志跟踪超时（秒），<=0 视为禁用")
	flags.IntVar(&opts.MaxRestart, "max-restart", 0, "Pod 最大允许重启次数")
	flags.BoolVar(&opts.CheckPodStatus, "check-pod-status", true, "Pod 状态强校验开关")
	flags.BoolVar(&opts.LogEnable, "log-enable", true, "实时日志监控开关")
	flags.StringSliceVar(&opts.LogErrKeywords, "log-err-keywords",
		[]string{"error", "panic", "fatal", "exception", "crash"}, "日志异常关键字（逗号分隔）")
	flags.StringSliceVar(&opts.LogIgnoreKeywords, "log-ignore-keywords", nil, "忽略的无害日志关键字（逗号分隔）")
	flags.IntVar(&opts.LogTail, "log-tail", 100, "回溯读取日志行数")
	flags.StringVar(&opts.LogErrorDir, "log-error-dir", ".", "错误日志落盘目录（文件：namespace-podname-date.log）")
	flags.StringVar(&opts.FeishuWebhook, "feishu-webhook", "", "飞书机器人 Webhook 地址，非空启用告警")
	flags.StringVar(&opts.FeishuSecret, "feishu-secret", "", "飞书签名校验密钥（HMAC-SHA256）")
	flags.IntVar(&opts.FeishuDedupWindowSec, "feishu-dedup-window", 30, "同 Pod+同事件告警去重窗口（秒）")
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
// -A/--all-namespaces 模式下无需指定资源，仅校验通用参数。
func parseArgs(c *cobra.Command, args []string, opts *options.Options) error {
	if opts.AllNamespaces {
		if len(args) > 0 {
			return fmt.Errorf("-A/--all-namespaces 模式下无需指定资源（将监听所有命名空间的 Deployment 更新）")
		}
	} else {
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
	if opts.DeployReadyTimeoutSec < 0 {
		return fmt.Errorf("--deploy-ready-timeout 不能为负数")
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
	if opts.LogErrorDir == "" {
		opts.LogErrorDir = "."
	}
	return nil
}

// run 主流程编排：构建客户端 -> 注册信号处理 -> Checker.Run -> 按退出码退出。
// -A 模式下走 watcher 常驻监听流程（所有命名空间 Deployment 更新自动检查）。
func run(c *cobra.Command, opts *options.Options) error {
	clientset, err := buildClient()
	if err != nil {
		fmt.Printf("[ERROR] 构建 Kubernetes 客户端失败: %v\n", err)
		return err
	}

	if opts.AllNamespaces {
		return runWatchMode(opts, clientset)
	}

	chk := checker.New(opts, clientset)

	// 信号处理：注册 SIGINT、SIGTERM 处理器。
	// 收到中断信号：调用所有 cancel 函数（cancel_deploy、全部 cancel_pod_ready、全部
	// cancel_pod_log），wg.Wait() 等待正在写日志的 goroutine 完成文件落盘，
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

// runWatchMode -A 常驻监听模式：注册信号处理（中断时停止监听并中断全部进行中的检查），
// 然后启动 watcher 监听所有命名空间的 Deployment 更新。
func runWatchMode(opts *options.Options, clientset kubernetes.Interface) error {
	w := watcher.New(opts, clientset)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Printf("[INFO] 收到中断信号，停止监听并中断所有进行中的检查\n")
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
