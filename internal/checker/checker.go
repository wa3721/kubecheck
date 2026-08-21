package checker

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"kubecheck/internal/feishu"
	"kubecheck/internal/options"
	"kubecheck/internal/recorder"
)

// 退出码定义
const (
	CodeOK            = 0   // 正常
	CodeParam         = 1   // 参数/前置错误（由 cobra 统一处理）
	CodeDeployTimeout = 2   // 阶段1：Deployment 滚动就绪超时（告警后直接退出，不再进入后续阶段）
	CodePodNotReady   = 3   // 最终：存在 Pod 异常退出 / 未产生任何 Pod
	CodeInterrupted   = 130 // 被中断（SIGINT/SIGTERM）
)

// Checker 聚合依赖与配置。
// 全局变量（线程安全），对应设计的：
//
//	hasPodFailure：是否有 pod 异常退出，初始 false
//	hasWarning：是否存在 pod 日志报错但 pod 存活，初始 false
//	wg：WaitGroup，等待所有 pod 相关 goroutine 执行完毕
type Checker struct {
	opts      *options.Options
	clientset kubernetes.Interface
	feishu    *feishu.FeishuAlert
	rec       *recorder.LogErrorRecorder

	mu            sync.Mutex
	hasPodFailure bool           // global_has_pod_failure
	hasWarning    bool           // global_has_warning
	wg            sync.WaitGroup // global wg

	// 全部超时上下文 cancel（cancel_deploy、全部 cancel_pod_ready、全部 cancel_pod_log），
	// 收到中断信号时统一取消
	cancelMu sync.Mutex
	cancels  []context.CancelFunc
}

// New 构造 Checker
func New(opts *options.Options, cs kubernetes.Interface) *Checker {
	return &Checker{
		opts:      opts,
		clientset: cs,
		feishu:    feishu.New(opts.FeishuWebhook, opts.FeishuSecret, opts.FeishuDedupWindow()),
		rec:       recorder.New(opts.LogErrorDir),
	}
}

// logf verbose 步骤日志：仅在 --verbose 时输出，带时间戳便于对照三阶段时序。
// 不改变任何检查逻辑与退出码，仅用于 e2e / 排障时观察工具走到了哪一步。
func (c *Checker) logf(format string, args ...interface{}) {
	if !c.opts.Verbose {
		return
	}
	fmt.Printf("[TRACE %s] %s\n", time.Now().Format("15:04:05"), fmt.Sprintf(format, args...))
}

// Run 主流程编排，对应预期设计的三阶段。
// rollout restart 由 CICD 触发，本工具只监视与告警，不做任何实际更新操作：
//
//	阶段1：informer 监听 Deployment 滚动更新完成
//	      条件：status.updatedReplicas == spec.replicas && status.readyReplicas == spec.replicas
//	      超时 -> 告警后 os.Exit(非 0)，不再进入后续阶段（前置阻断）
//	      满足 -> 提取本次发布（new_rs，即最大 revision 的 ReplicaSet），后续只处理其下 Pod
//	阶段2：new_rs 下每个 Pod 独立 goroutine，各自独立 pod_ready_timeout 等待进入 Running；
//	      超时 -> 置 global_has_pod_failure 并发送告警，结束该 goroutine
//	阶段3：每个 Pod 从 Running 时刻起独立 pod_log_watch_timeout 观察日志（follow 增量写文件）；
//	      命中错误关键字 -> pod_local_has_error_log；容器异常退出（退出码 != 0）-> global_has_pod_failure
//	最终：主 goroutine wg.Wait() 等待全部 goroutine（含日志落盘）后按全局状态聚合退出
//
// 重要备注（逻辑约束，写进注释）：
//   - 阶段1 是前置阻断：必须 deployment 滚动完全完成之后，才开始处理 pod，
//     这是当前这套流程固有的短板：慢启动应用会丢失 pod 早期日志；
//     如果后续要优化，把阶段1改为并行辅助观测。
//   - 阶段2 每个 pod 拥有独立 pod_ready_timeout，不是全局共享。
//   - 阶段3 每个 pod 从自己变为 Running 那一刻开始计时 pod_log_watch_timeout，每个 pod 独立超时上下文。
//   - 任意 pod 故障不会直接 os.Exit，必须等待 wg 全部完成、日志落盘完毕，再做进程退出。
//   - 严格过滤旧 RS 的 pod，不处理不属于 new_rs 的 pod。
func (c *Checker) Run(ctx context.Context) int {
	if c.opts.Verbose {
		fmt.Printf("[INFO] 开始检查 deployment/%s (ns=%s)\n", c.opts.ResourceName, c.opts.Namespace)
	}
	c.logf("阶段1: 等待 Deployment %s/%s 滚动更新完成（deploy-ready-timeout=%d 秒）",
		c.opts.Namespace, c.opts.ResourceName, c.opts.DeployReadyTimeoutSec)

	// ---------- 阶段1：Deployment 滚动更新完成（前置阻断）----------
	if !c.waitDeploymentReady(ctx) {
		c.logf("阶段1: 滚动更新未完成（超时 %d 秒），发送告警退出 code=%d",
			c.opts.DeployReadyTimeoutSec, CodeDeployTimeout)
		msg := fmt.Sprintf("Deployment %s/%s 在 %d 秒内滚动更新未完成（updatedReplicas/readyReplicas 未达到 spec.replicas，或旧副本未缩容/controller 未观察到最新 spec）",
			c.opts.Namespace, c.opts.ResourceName, c.opts.DeployReadyTimeoutSec)
		c.alert(feishu.EventDeployTimeout, c.opts.ResourceName, msg)
		return CodeDeployTimeout
	}
	c.logf("阶段1: Deployment 滚动更新完成")

	if !c.opts.CheckPodStatus {
		// 不追踪 Pod，仅 Deployment 滚动完成即通过
		fmt.Printf("[INFO] Deployment %s/%s 滚动更新完成（未启用 Pod 追踪）\n", c.opts.Namespace, c.opts.ResourceName)
		return CodeOK
	}

	// ---------- 阶段2 前置：定位本次发布（new_rs）下的 Pod ----------
	// 注意：API 对象无 deployment.status.newReplicaSet 字段（该字段仅存在于 kubectl 输出），
	// 这里以"Deployment 控制下 revision 最大的 ReplicaSet"等价实现 new_rs 的提取。
	pods, err := c.waitForTargetPods(ctx)
	if err != nil {
		fmt.Printf("[ERROR] 获取目标 Pod 失败: %v\n", err)
		return CodeParam
	}
	if len(pods) == 0 {
		fmt.Printf("[WARN] 未在超时内找到 Deployment %s/%s 对应的 Pod\n", c.opts.Namespace, c.opts.ResourceName)
		msg := fmt.Sprintf("Deployment %s/%s 滚动更新后未产生任何 Pod", c.opts.Namespace, c.opts.ResourceName)
		c.alert(feishu.EventPodStatus, c.opts.ResourceName, msg)
		return CodePodNotReady
	}

	// ---------- 阶段2 + 阶段3：每个 Pod 独立 goroutine（就绪监控 + 日志追踪）----------
	c.watchPods(ctx, pods)

	// ---------- 主 goroutine 阻塞等待：全部 pod 就绪 goroutine、全部日志追踪 goroutine 完成 ----------
	c.wg.Wait()

	// ---------- 最终结果判断 ----------
	if c.hasPodFailure {
		c.logf("最终: 存在 Pod 异常，置 pod failure -> code=%d", CodePodNotReady)
		c.alert(feishu.EventContainerExit, c.opts.ResourceName,
			fmt.Sprintf("Deployment %s/%s 存在 Pod 异常退出，请及时处理", c.opts.Namespace, c.opts.ResourceName))
		return CodePodNotReady
	}
	if c.hasWarning {
		// 发送警告提醒运维关注日志，退出码 = 0
		c.logf("最终: 存在日志报错但 Pod 存活，置 warning -> code=%d", CodeOK)
		c.alert(feishu.EventPendingCheck, c.opts.ResourceName,
			fmt.Sprintf("Deployment %s/%s 存在 Pod 日志报错但 Pod 存活，建议关注日志", c.opts.Namespace, c.opts.ResourceName))
		return CodeOK
	}
	fmt.Printf("[INFO] Deployment %s/%s 全部 Pod 运行正常\n", c.opts.Namespace, c.opts.ResourceName)
	return CodeOK
}

// waitDeploymentReady 阶段1：用 informer 监听 Deployment，直到滚动更新完成或超时。
// 判定条件（分支 B）：见 rolloutComplete——除 updatedReplicas/readyReplicas 达到
// spec.replicas 外，还要求 status.replicas == spec.replicas（旧 RS 已缩容）且
// observedGeneration >= generation（controller 已观察到最新 spec）。
//
// 事件驱动而非轮询：Deployment 状态每次变化（Add/Update）立即判定，状态不变时不空转，
// 仅靠事件通知；首次缓存同步后再补一次当前状态检查，避免依赖后续事件。
func (c *Checker) waitDeploymentReady(ctx context.Context) bool {
	timeout := c.opts.DeployReadyTimeout()
	if timeout <= 0 {
		// 禁用超时则立即判定当前状态
		return c.isRolloutComplete(ctx)
	}

	// 目标 Deployment 达到滚动完成条件时通知
	readyCh := make(chan struct{}, 1)
	notifyReady := func() {
		select {
		case readyCh <- struct{}{}:
		default:
		}
	}

	factory := informers.NewSharedInformerFactoryWithOptions(c.clientset, 0,
		informers.WithNamespace(c.opts.Namespace))
	informer := factory.Apps().V1().Deployments().Informer()
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			if d, ok := obj.(*appsv1.Deployment); ok && d.Name == c.opts.ResourceName && rolloutComplete(d) {
				notifyReady()
			}
		},
		UpdateFunc: func(_, newObj interface{}) {
			if d, ok := newObj.(*appsv1.Deployment); ok && d.Name == c.opts.ResourceName && rolloutComplete(d) {
				notifyReady()
			}
		},
	})
	if err != nil {
		// 事件处理器注册失败：退化为立即判定一次
		return c.isRolloutComplete(ctx)
	}

	ctxTimeout, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	factory.Start(ctxTimeout.Done())
	if !cache.WaitForCacheSync(ctxTimeout.Done(), informer.HasSynced) {
		return false
	}
	// 首次同步后立即检查一次当前状态（informer 缓存可能已包含已完成状态）
	if obj, exists, err := informer.GetStore().GetByKey(c.opts.Namespace + "/" + c.opts.ResourceName); err == nil && exists {
		if d, ok := obj.(*appsv1.Deployment); ok && rolloutComplete(d) {
			return true
		}
	}
	statusTicker := time.NewTicker(2 * time.Second)
	defer statusTicker.Stop()
	for {
		select {
		case <-readyCh:
			return true
		case <-ctxTimeout.Done():
			return false
		case <-statusTicker.C:
			// verbose 时周期性打印滚动进度，便于观察走到了哪一步
			c.logDeploymentStatus(ctxTimeout)
		}
	}
}

// logDeploymentStatus verbose 阶段1 状态快照：打印 Deployment 当前滚动进度
func (c *Checker) logDeploymentStatus(ctx context.Context) {
	d, err := c.clientset.AppsV1().Deployments(c.opts.Namespace).Get(ctx, c.opts.ResourceName, metav1.GetOptions{})
	if err != nil {
		c.logf("阶段1: 获取 Deployment 状态失败: %v", err)
		return
	}
	replicas := int32(0)
	if d.Spec.Replicas != nil {
		replicas = *d.Spec.Replicas
	}
	c.logf("阶段1: 滚动进度 spec=%d status(replicas=%d updated=%d ready=%d available=%d) gen=%d/%d complete=%v",
		replicas, d.Status.Replicas, d.Status.UpdatedReplicas, d.Status.ReadyReplicas,
		d.Status.AvailableReplicas, d.Status.ObservedGeneration, d.Generation, rolloutComplete(d))
}

// isRolloutComplete 读取 Deployment 最新状态并判定滚动更新完成
func (c *Checker) isRolloutComplete(ctx context.Context) bool {
	d, err := c.clientset.AppsV1().Deployments(c.opts.Namespace).Get(ctx, c.opts.ResourceName, metav1.GetOptions{})
	if err != nil {
		return false
	}
	return rolloutComplete(d)
}

// rolloutComplete 滚动更新完成判定（阶段1 分支 B），对齐 kubectl rollout status 的收敛判定：
//
//	observedGeneration >= generation（controller 已观察到最新 spec）
//	status.replicas == spec.replicas（旧 RS 已全部缩容，无多余副本）
//	status.updatedReplicas == spec.replicas（所有副本都已使用新模板）
//	status.readyReplicas == spec.replicas（所有新副本都已就绪）
//
// 必须与 spec.replicas 比较而非 status.replicas：
//   - 滚动初始瞬态（controller 尚未更新 status）status 各字段可能全为 0，
//     与 status.replicas 比较会得到 0==0 && 0==0 的错误通过；
//   - 滚动进行中，旧 RS 的 Pod 可能仍处于 Ready 状态，此时 readyReplicas 会“恰好等于”
//     spec.replicas（就绪的是旧 Pod），必须再校验 status.replicas == spec.replicas
//     才能排除旧 RS 未缩容的情况（否则阶段1 在“新 Pod 未就绪、旧 Pod 仍在”时误判完成）。
func rolloutComplete(d *appsv1.Deployment) bool {
	replicas := int32(0)
	if d.Spec.Replicas != nil {
		replicas = *d.Spec.Replicas
	}
	return d.Status.ObservedGeneration >= d.Generation &&
		d.Status.Replicas == replicas &&
		d.Status.UpdatedReplicas == replicas &&
		d.Status.ReadyReplicas == replicas
}

// listTargetPods 查找 Deployment 本次发布（new_rs，即最大 revision 的 ReplicaSet）对应的 Pod。
// rollout 期间新旧 ReplicaSet 共存：只检查最新 revision 的 RS（即本次发布的新 Pod），
// 严格过滤旧 RS 的 Pod，避免把正在缩容的旧版本 Pod 一并纳入检查。
// 同时过滤处于删除中（DeletionTimestamp 非空）的 Pod：这类 Pod phase 可能仍是 Running，
// 若纳入检查会被误判为"已就绪的正常 Pod"，从而跳过对真正异常（如 Pending）Pod 的检测。
// 若无法判定最新 RS，退化为使用 Deployment 的 spec.selector。
func (c *Checker) listTargetPods(ctx context.Context) ([]corev1.Pod, error) {
	d, err := c.clientset.AppsV1().Deployments(c.opts.Namespace).Get(ctx, c.opts.ResourceName, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	rsList, err := c.clientset.AppsV1().ReplicaSets(c.opts.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, err
	}

	// 在 deployment 控制的 RS 中，选出 revision 最大的（本次发布的新 RS）
	var newestRS *appsv1.ReplicaSet
	var newestRev int64 = -1
	for i := range rsList.Items {
		rs := &rsList.Items[i]
		if !(metav1.IsControlledBy(rs, d) || ownerMatches(rs, d)) {
			continue
		}
		rev := rsRevision(rs)
		if rev > newestRev {
			newestRev = rev
			newestRS = rs
		}
	}

	if newestRS != nil {
		c.logf("定位本次发布 new_rs=%s (revision=%d)", newestRS.Name, newestRev)
	}
	var sel labels.Selector
	if newestRS != nil && len(newestRS.Spec.Selector.MatchLabels) > 0 {
		// 新 RS 的 selector 含 pod-template-hash，只会命中本次发布的新 Pod
		sel = labels.SelectorFromSet(newestRS.Spec.Selector.MatchLabels)
	} else if d.Spec.Selector != nil && len(d.Spec.Selector.MatchLabels) > 0 {
		// 退化为 Deployment 的 spec.selector
		sel = labels.SelectorFromSet(d.Spec.Selector.MatchLabels)
	}
	if sel == nil {
		return nil, nil
	}
	podList, err := c.clientset.CoreV1().Pods(c.opts.Namespace).List(ctx, metav1.ListOptions{
		LabelSelector: sel.String(),
	})
	if err != nil {
		return nil, err
	}
	pods := make([]corev1.Pod, 0, len(podList.Items))
	for _, p := range podList.Items {
		// 过滤正在删除的 Pod（Terminating，DeletionTimestamp 非空）：
		// 其 phase 可能仍是 Running，会被阶段2 误判为健康，导致真正的异常 Pod 漏检
		if p.DeletionTimestamp != nil {
			continue
		}
		pods = append(pods, p)
	}
	if c.opts.Verbose {
		for _, p := range pods {
			c.logf("目标 Pod: %s phase=%s", p.Name, p.Status.Phase)
		}
	}
	return pods, nil
}

// rsRevision 读取 ReplicaSet 的 deployment.kubernetes.io/revision annotation
func rsRevision(rs *appsv1.ReplicaSet) int64 {
	v, ok := rs.Annotations["deployment.kubernetes.io/revision"]
	if !ok {
		return 0
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

// waitForTargetPods 阶段2 前置：在 pod_ready_timeout 窗口内反复查找目标 Pod。
// Deployment 刚完成滚动（阶段1通过）时，新 ReplicaSet 的 Pod 可能尚未被创建，
// 此时不应立即判定"无 Pod 异常"，而应等待其出现；超时后仍无 Pod 再交由调用方处理。
func (c *Checker) waitForTargetPods(ctx context.Context) ([]corev1.Pod, error) {
	timeout := c.opts.PodReadyTimeout()
	if timeout <= 0 {
		// 禁用超时则立即判定当前状态
		return c.listTargetPods(ctx)
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

	waited := 0
	for {
		pods, err := c.listTargetPods(ctx)
		if err != nil {
			return nil, err
		}
		if len(pods) > 0 {
			c.logf("阶段2 前置: 等待 %d 秒后找到 %d 个目标 Pod", waited, len(pods))
			return pods, nil
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			// 超时仍无 Pod，返回空交由上层告警
			c.logf("阶段2 前置: 超时（已等待 %d 秒）仍未找到目标 Pod", waited)
			return nil, nil
		case <-time.After(1 * time.Second):
			// 周期性轮询，等待 Pod 被创建
			waited++
			if waited%5 == 0 {
				c.logf("阶段2 前置: 等待目标 Pod 出现，已等待 %d 秒", waited)
			}
		}
	}
}

// watchPods 阶段2+3：为每个 Pod 启动独立 goroutine，全部登记到全局 wg。
func (c *Checker) watchPods(ctx context.Context, pods []corev1.Pod) {
	for i := range pods {
		p := pods[i]
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.trackPod(ctx, p)
		}()
	}
}

// trackPod 单个 Pod：阶段2（等待进入 Running，独立 pod_ready_timeout）成功后，
// 进入阶段3（日志观察，独立 pod_log_watch_timeout，从 Running 时刻计时）。
func (c *Checker) trackPod(ctx context.Context, pod corev1.Pod) {
	// ---- 阶段2：等待 Pod 进入 Running（每个 Pod 独立超时，非全局共享）----
	c.logf("阶段2: 等待 Pod %s 进入 Running（pod-ready-timeout=%d 秒）", pod.Name, c.opts.PodReadyTimeoutSec)
	readyCtx, cancelReady := context.WithTimeout(context.Background(), c.opts.PodReadyTimeout())
	c.registerCancel(cancelReady)
	defer cancelReady()

	runningAt, ok := c.waitPodRunning(readyCtx, pod)
	if !ok {
		if readyCtx.Err() == context.Canceled {
			// 被中断/取消：静默返回，由中断流程统一告警
			return
		}
		// 分支1：pod_ready_timeout 超时仍未进入 Running。
		// 置 global_has_pod_failure = true（未进入 Running 视为 pod 异常），
		// 发送告警并结束该 goroutine
		c.logf("阶段2: Pod %s 未进入 Running（超时 %d 秒），置 pod failure", pod.Name, c.opts.PodReadyTimeoutSec)
		c.setPodFailure()
		c.alert(feishu.EventPodStatus, pod.Name,
			fmt.Sprintf("Pod %s 命名空间 %s 未进入 running（超时 %d 秒）", pod.Name, pod.Namespace, c.opts.PodReadyTimeoutSec))
		return
	}

	// ---- 阶段3：日志追踪（每个 Pod 独立超时，计时起点 = 变为 Running 的时刻）----
	if !c.opts.LogEnable {
		return
	}
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.watchPodLog(ctx, pod, runningAt)
	}()
}

// waitPodRunning 阶段2：在 ctx 有效期内轮询 Pod 状态，直到 Phase == Running。
// 每次重新 Get 最新状态（不信任初始快照）；返回进入 Running 的时刻（阶段3 计时起点）。
func (c *Checker) waitPodRunning(ctx context.Context, pod corev1.Pod) (time.Time, bool) {
	start := time.Now()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		p, err := c.clientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err == nil && p.Status.Phase == corev1.PodRunning {
			c.logf("阶段2: Pod %s 进入 Running（耗时 %.1f 秒）", pod.Name, time.Since(start).Seconds())
			return time.Now(), true
		}
		// verbose：每 5 秒打印一次当前 phase，便于观察 Pod 卡在哪个状态
		if err == nil && int(time.Since(start).Seconds())%5 == 0 && p.Status.Phase != "" {
			c.logf("阶段2: Pod %s 当前 phase=%s（已等待 %.1f 秒）", pod.Name, p.Status.Phase, time.Since(start).Seconds())
		}
		select {
		case <-ctx.Done():
			return time.Time{}, false
		case <-ticker.C:
		}
	}
}

// watchPodLog 阶段3：在 pod_log_watch_timeout 内以 follow 模式采集该 Pod 容器日志，
// 增量写入本地日志文件。两条独立路径并行：
//
//	① 日志流：rec.TrackLog 持续增量落盘，命中错误关键字 -> pod_local_has_error_log
//	② 容器状态：周期 Get Pod，容器 Terminated 且退出码 != 0 -> global_has_pod_failure
//
// 结束时机：
//   - ② 命中 -> 停止日志采集（cancelLog），等待剩余缓冲日志全部落盘，发送异常告警
//   - 日志观察窗口超时 -> 若 pod_local_has_error_log，置 global_has_warning
func (c *Checker) watchPodLog(ctx context.Context, pod corev1.Pod, runningAt time.Time) {
	if c.opts.LogCheckTimeoutSec <= 0 {
		return
	}
	// 计时起点 = Pod 变为 Running 的时刻（每 Pod 独立超时上下文）
	remaining := c.opts.LogCheckTimeout() - time.Since(runningAt)
	if remaining < 0 {
		remaining = 0
	}
	logCtx, cancelLog := context.WithTimeout(context.Background(), remaining)
	c.registerCancel(cancelLog)
	defer cancelLog()
	c.logf("阶段3: 开始观察 Pod %s 日志（log-check-timeout=%.0f 秒）", pod.Name, remaining.Seconds())

	var (
		mu                  sync.Mutex
		podLocalHasErrorLog bool
	)
	logDone := make(chan struct{})
	go func() {
		defer close(logDone)
		_, hit, err := c.rec.TrackLog(logCtx, c.clientset, &pod, c.opts.LogErrKeywords, c.opts.LogIgnoreKeywords, c.opts.LogTail)
		if err != nil && c.opts.Verbose {
			fmt.Printf("[WARN] 跟踪 %s/%s 日志出错: %v\n", pod.Namespace, pod.Name, err)
		}
		mu.Lock()
		podLocalHasErrorLog = hit
		mu.Unlock()
	}()

	// 容器退出（退出码 != 0）/ 重启超限 检测通道；exitCode < 0 表示重启超限
	exitCh := make(chan int32, 1)
	go func() {
		checkExit := func() {
			p, err := c.clientset.CoreV1().Pods(pod.Namespace).Get(logCtx, pod.Name, metav1.GetOptions{})
			if err != nil {
				return
			}
			if code, exited := containerExitCode(p); exited {
				select {
				case exitCh <- code:
				default:
				}
				return
			}
			if c.opts.MaxRestart > 0 && int(totalRestartCount(p)) > c.opts.MaxRestart {
				select {
				case exitCh <- -1:
				default:
				}
			}
		}
		// 立即检查一次（Pod 可能在进入 Running 前已异常退出，避免被日志超时掩盖）
		checkExit()
		ticker := time.NewTicker(1 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-logCtx.Done():
				return
			case <-ticker.C:
				checkExit()
			}
		}
	}()

	for {
		select {
		case code := <-exitCh:
			// ② pod 容器异常退出（或重启超限）：
			// 停止日志采集，把剩余缓冲区日志全部写入日志文件落盘
			c.logf("阶段3: Pod %s 容器异常退出（code=%d），停止日志观察", pod.Name, code)
			cancelLog()
			c.waitLogFlush(logDone)
			c.setPodFailure()
			c.alertPodExit(pod, code)
			return
		case <-logCtx.Done():
			// ① 日志观察窗口超时结束
			c.waitLogFlush(logDone)
			if logCtx.Err() == context.Canceled {
				// 被中断取消：不置 warning，由中断流程统一处理
				return
			}
			mu.Lock()
			hit := podLocalHasErrorLog
			mu.Unlock()
			if hit {
				// pod 日志报错但 pod 存活 -> global_has_warning = true
				c.logf("阶段3: Pod %s 日志命中错误关键字，置 warning", pod.Name)
				c.setWarning()
			} else {
				c.logf("阶段3: Pod %s 日志观察窗口结束，未命中错误关键字", pod.Name)
			}
			return
		}
	}
}

// waitLogFlush 等待日志跟踪 goroutine 完成文件落盘（带兜底超时，避免异常卡死）
func (c *Checker) waitLogFlush(logDone chan struct{}) {
	select {
	case <-logDone:
	case <-time.After(5 * time.Second):
	}
}

// containerExitCode 若任一容器处于 Terminated 且退出码 != 0，返回 (退出码, true)
func containerExitCode(p *corev1.Pod) (int32, bool) {
	for _, cs := range p.Status.ContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
			return cs.State.Terminated.ExitCode, true
		}
	}
	return 0, false
}

// alertPodExit 容器异常退出 / 重启超限告警
func (c *Checker) alertPodExit(pod corev1.Pod, code int32) {
	if code < 0 {
		c.alert(feishu.EventRestartLimit, pod.Name,
			fmt.Sprintf("Pod %s 命名空间 %s 重启次数超过上限（%d 次）", pod.Name, pod.Namespace, c.opts.MaxRestart))
		return
	}
	c.alert(feishu.EventContainerExit, pod.Name,
		fmt.Sprintf("Pod %s 命名空间 %s 容器异常退出（退出码 %d），日志已落盘: %s",
			pod.Name, pod.Namespace, code, c.rec.RecordedPath(pod.Namespace, pod.Name)))
}

// Interrupt 收到中断信号（SIGINT/SIGTERM）时调用：
// 调用所有 cancel 函数（cancel_deploy、全部 cancel_pod_ready、全部 cancel_pod_log），
// wg.Wait() 等待正在写日志的 goroutine 完成文件落盘，组装中断告警发送（飞书/降级控制台）。
func (c *Checker) Interrupt() {
	c.cancelAll()
	c.wg.Wait()
	c.alert(feishu.EventInterrupted, c.opts.ResourceName,
		fmt.Sprintf("kubectl-check 收到中断信号，检查被中止（Deployment %s/%s）", c.opts.Namespace, c.opts.ResourceName))
}

// registerCancel 登记一个超时上下文 cancel，供中断时统一取消
func (c *Checker) registerCancel(cancel context.CancelFunc) {
	c.cancelMu.Lock()
	defer c.cancelMu.Unlock()
	c.cancels = append(c.cancels, cancel)
}

// cancelAll 调用全部已登记的 cancel（cancel_deploy、全部 cancel_pod_ready、全部 cancel_pod_log）
func (c *Checker) cancelAll() {
	c.cancelMu.Lock()
	defer c.cancelMu.Unlock()
	for _, cancel := range c.cancels {
		cancel()
	}
}

func (c *Checker) setPodFailure() {
	c.mu.Lock()
	c.hasPodFailure = true
	c.mu.Unlock()
}

func (c *Checker) setWarning() {
	c.mu.Lock()
	c.hasWarning = true
	c.mu.Unlock()
}

// alert 发送告警：统一降级逻辑——webhook 为空时控制台打印；
// webhook 非空时走飞书 http 调用，调用失败由 feishu 层降级控制台打印（不 panic）。
func (c *Checker) alert(event string, key, detail string) {
	if c.feishu.Enabled() {
		c.feishu.Send(event, key, detail)
		return
	}
	fmt.Printf("[ALERT] %s | %s: %s\n", event, key, detail)
}

// ownerMatches 判断 ReplicaSet 是否由给定 Deployment 直接控制
func ownerMatches(rs *appsv1.ReplicaSet, d *appsv1.Deployment) bool {
	for _, o := range rs.OwnerReferences {
		if o.Kind == "Deployment" && o.Name == d.Name && o.UID == d.UID {
			return true
		}
	}
	return false
}

// totalRestartCount 汇总 Pod 所有容器的重启次数
func totalRestartCount(p *corev1.Pod) int32 {
	var n int32
	for _, cs := range p.Status.ContainerStatuses {
		n += cs.RestartCount
	}
	return n
}
