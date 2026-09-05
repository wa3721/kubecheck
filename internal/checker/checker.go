package checker

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"

	"kubecheck/internal/console"
	"kubecheck/internal/feishu"
	"kubecheck/internal/llm"
	"kubecheck/internal/options"
	"kubecheck/internal/recorder"
)

// 退出码定义
const (
	CodeOK          = 0   // 正常
	CodeParam       = 1   // 参数/前置错误（由 cobra 统一处理）
	CodeNoTargetPod = 2   // 目标定位：未找到可检查的目标 Pod（告警后直接退出，不进入 Pod 校验）
	CodePodNotReady = 3   // 最终：存在 Pod 异常退出 / 未产生任何 Pod
	CodeInterrupted = 130 // 被中断（SIGINT/SIGTERM）
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
	judge     recorder.ErrorJudge // LLM 日志错误仲裁；nil 或未启用 -> 关键字即真（现行行为）

	mu            sync.Mutex
	hasPodFailure bool           // global_has_pod_failure
	hasWarning    bool           // global_has_warning
	wg            sync.WaitGroup // global wg

	// 全部超时上下文 cancel（全部 cancel_pod_ready、全部 cancel_pod_log），
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
		rec:       recorder.New(opts.LogErrorDir, opts.LogDump, opts.LogConsole),
		judge:     llm.New(opts.LLMEndpoint, opts.LLMModel, opts.LLMApiKey, opts.LLMTimeout()),
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

// Run 主流程编排，对应预期设计的目标定位 + 两阶段校验。
// rollout 由 CICD 触发，本工具只监视与告警，不做任何实际更新操作：
//
//	目标定位（仅校验 Pod 层级，不校验 Deployment 滚动状态）：定位本次发布（new_rs，
//	      最大 revision 的 ReplicaSet）的目标 Pod，直接进入 Pod 层级校验；
//	      replicas > 0 时在 pod-ready-timeout 窗口内等待目标 Pod 出现（覆盖"发布刚触发、
//	      新 RS 已创建但 Pod 尚未创建"的竞态窗口）；replicas=0 无目标可等，立即退出；
//	      窗口内未出现 -> 告警后退出（code=2）
//	阶段2：目标 Pod 各自独立 pod_ready_timeout 等待就绪（Ready 条件，含就绪探针校验）；
//	      超时 -> 置 global_has_pod_failure 并发送告警，结束该 goroutine
//	阶段3：每个 Pod 从首次 Running 时刻起独立 log_check_timeout 观察日志（follow 增量），
//	      与阶段2 的 Ready 等待并行（有探针时 0/1 Running 期间两阶段共同作用）；
//	      命中错误关键字 -> LLM 仲裁真伪（未启用则关键字即真）；存在真错误 ->
//	      global_has_warning；容器异常退出（退出码 != 0）-> global_has_pod_failure 并
//	      联动取消阶段2（Ready 前崩溃也能立即精确告警，无需等就绪超时）
//	后置发现（多副本滚动场景）：目标定位只要等到第 1 个新 Pod 即返回（不等待凑齐
//	      期望副本数），首发 Pod 立即进入完整检查（阶段2+3）；滚动更新中后续逐个
//	      创建的新 Pod 由后置发现 goroutine 轮询补录（pod-ready-timeout 窗口内，
//	      已跟踪数达到 spec.replicas 即收敛停止），后置 Pod 仅执行阶段2 就绪检查
//	      （不启用阶段3 日志观察/退出检测——首发已承担"本次发布内容是否异常"判定，
//	      后置只验证滚动收敛），Ready 超时与首发同权重置 pod failure（code=3）；
//	      窗口结束仍未凑齐不告警不置失败（不校验滚动收敛，由 rollout status 负责）
//	最终：主 goroutine wg.Wait() 等待全部 goroutine（含日志落盘）后按全局状态聚合退出
//
// 重要备注（逻辑约束，写进注释）：
//   - 不校验 Deployment 的超时与滚动状态，仅校验 Pod 层级（就绪/日志/退出）；
//     滚动 surge 期间的临时副本也可能被纳入名单，由阶段2/3 的 DeletionTimestamp 防护兜底
//     （controller 删除的 Pod 不计为异常）。
//   - 阶段2 每个 pod 拥有独立 pod_ready_timeout，不是全局共享。
//   - 阶段3 从首次 Running 启动（非 Ready）：探针未通过（0/1 Running）期间日志观察
//     与退出检测已在运行；两阶段通过 podCtx 联动——任一终态取消另一路，同一故障仅一条告警。
//   - 阶段3 每个 pod 从自己变为 Running 那一刻开始计时 log_check_timeout，每个 pod 独立超时上下文。
//   - 任意 pod 故障不会直接 os.Exit，必须等待 wg 全部完成、日志落盘完毕，再做进程退出。
//   - 严格过滤旧 RS 的 pod，不处理不属于 new_rs 的 pod。
func (c *Checker) Run(ctx context.Context) int {
	fmt.Println(console.Green(fmt.Sprintf("[INFO] 开始检查 deployment/%s (ns=%s)", c.opts.ResourceName, c.opts.Namespace)))
	c.logf("定位 Deployment %s/%s 本次发布的目标 Pod（最新 ReplicaSet）",
		c.opts.Namespace, c.opts.ResourceName)

	// ---------- 目标定位：等待目标 Pod 出现（不校验 Deployment 滚动状态/收敛）----------
	pods, replicas, noTarget, err := c.locateTargetPods(ctx)
	if err != nil {
		fmt.Println(console.Red(fmt.Sprintf("[ERROR] 获取目标 Pod 失败: %v", err)))
		return CodeParam
	}
	if noTarget {
		c.logf("未找到目标 Pod，发送告警退出 code=%d", CodeNoTargetPod)
		msg := fmt.Sprintf("Deployment %s/%s 未找到目标 Pod（本次发布的新 RS 无 Pod，请确认发布已触发且副本数大于 0）",
			c.opts.Namespace, c.opts.ResourceName)
		c.alert(feishu.EventNoTargetPod, c.opts.ResourceName, msg)
		return CodeNoTargetPod
	}
	c.logf("已定位 %d 个目标 Pod", len(pods))

	if !c.opts.CheckPodStatus {
		// 不追踪 Pod，仅目标定位完成即通过
		fmt.Println(console.Green(fmt.Sprintf("[INFO] Deployment %s/%s 目标定位完成（未启用 Pod 追踪）", c.opts.Namespace, c.opts.ResourceName)))
		return CodeOK
	}

	// ---------- 阶段2 + 阶段3：首发 Pod 完整检查（每个 Pod 独立 goroutine）----------
	// tracked 记录已启动跟踪的 Pod 名（首发 + 后置发现补录），仅供后置发现器去重，
	// 只被主 goroutine 写入（启动前）与发现器单 goroutine 读写，无并发竞争
	tracked := make(map[string]struct{}, len(pods))
	for i := range pods {
		tracked[pods[i].Name] = struct{}{}
	}
	c.watchPods(pods, true)

	// ---------- 后置发现器：滚动中逐个创建的后置 Pod，出现一个跟踪一个（仅就绪检查）----------
	// 使用定位时刻的 replicas 快照（发布过程中 scale 扩容/缩容不在此工具的职责范围）；
	// 首发已凑齐期望副本数时不启动发现器，行为与历史版本一致
	if int(replicas) > len(tracked) {
		c.wg.Add(1)
		go c.discoverLatePods(replicas, tracked)
	}

	// ---------- 主 goroutine 阻塞等待：全部 pod 就绪 goroutine、全部日志追踪 goroutine 完成 ----------
	c.wg.Wait()

	// ---------- 最终结果判断 ----------
	// 异常告警已在各失败路径即时发送（阶段2 未就绪 -> EventPodStatus；
	// 阶段3 容器退出/重启超限 -> alertPodExit 的 EventContainerExit/EventRestartLimit），
	// 聚合处只决定退出码，不再重复发送汇总告警（避免同一故障触发两条 ALERT）
	if c.hasPodFailure {
		c.logf("最终: 存在 Pod 异常，置 pod failure -> code=%d", CodePodNotReady)
		return CodePodNotReady
	}
	if c.hasWarning {
		// 发送警告提醒运维关注日志，退出码 = 0
		c.logf("最终: 存在真错误日志但 Pod 存活，置 warning -> code=%d", CodeOK)
		c.alert(feishu.EventPendingCheck, c.opts.ResourceName,
			fmt.Sprintf("Deployment %s/%s 存在 Pod 日志报错但 Pod 存活，建议关注日志", c.opts.Namespace, c.opts.ResourceName))
		return CodeOK
	}
	fmt.Println(console.Green(fmt.Sprintf("[INFO] Deployment %s/%s 全部 Pod 运行正常", c.opts.Namespace, c.opts.ResourceName)))
	return CodeOK
}

// locateTargetPods 目标定位（仅校验 Pod 层级，不校验 Deployment 滚动状态/收敛）：
// 在 pod-ready-timeout 窗口内 1s 轮询 listTargetPods，等待本次发布（new_rs）的目标 Pod 出现。
// 覆盖"发布刚触发的竞态窗口"——kubectl rollout restart 后立即启动本工具时，新 RS 已被
// controller 创建，但新 RS 的 Pod 尚未创建（通常滞后 1-3 秒），单次查询会误判"未找到目标 Pod"。
//
// 分支：
//   - spec.replicas == 0：无目标可等（controller 不会创建任何 Pod），立即返回 noTarget=true
//   - spec.replicas > 0：先单次查询；未命中则轮询直至目标 Pod 出现或 pod-ready-timeout 超时
//     （超时 -> noTarget=true，由 Run 告警 EventNoTargetPod 并以 code=2 退出）
//
// 返回的 replicas 为定位时刻 spec.replicas 的快照，供 Run 判定是否启动后置发现器
// （首发数量未达期望副本数 -> 滚动中后续 Pod 逐个创建，需补录跟踪）。
//
// 注意：等待的是"目标 Pod 存在"而非"就绪"——Pending/拉镜像中的 Pod 也算定位成功，
// 就绪校验由阶段2 承担；spec.replicas 是 spec 字段而非滚动状态，不构成对 Deployment
// 滚动收敛的校验。err 非 nil 表示资源访问失败（CodeParam）。
func (c *Checker) locateTargetPods(ctx context.Context) (pods []corev1.Pod, replicas int32, noTarget bool, err error) {
	d, err := c.clientset.AppsV1().Deployments(c.opts.Namespace).Get(ctx, c.opts.ResourceName, metav1.GetOptions{})
	if err != nil {
		return nil, 0, false, err
	}
	replicas = int32(0)
	if d.Spec.Replicas != nil {
		replicas = *d.Spec.Replicas
	}
	if replicas == 0 {
		// replicas=0：无目标可等，立即判定（不需要轮询）
		return nil, 0, true, nil
	}

	// 立即查询一次（覆盖"启动时目标 Pod 已存在"的场景）
	pods, err = c.listTargetPods(ctx)
	if err != nil {
		return nil, 0, false, err
	}
	if len(pods) > 0 {
		return pods, replicas, false, nil
	}

	// 轮询等待目标 Pod 出现（发布刚触发的竞态窗口）
	deadline := time.NewTimer(c.opts.PodReadyTimeout())
	defer deadline.Stop()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	waited := 0
	for {
		select {
		case <-ctx.Done():
			return nil, 0, false, ctx.Err()
		case <-deadline.C:
			c.logf("等待 %d 秒未出现目标 Pod", waited+1)
			return nil, 0, true, nil
		case <-ticker.C:
			waited++
			pods, err = c.listTargetPods(ctx)
			if err != nil {
				return nil, 0, false, err
			}
			if len(pods) > 0 {
				c.logf("目标 Pod 已出现（等待 %d 秒）", waited)
				return pods, replicas, false, nil
			}
			if waited%5 == 0 {
				c.logf("等待目标 Pod 出现，已等待 %d 秒", waited)
			}
		}
	}
}

// listTargetPods 单次查询本次发布（new_rs，即最大 revision 的 ReplicaSet）对应的 Pod
// （由 locateTargetPods 在轮询窗口内调用）——不校验 Deployment 滚动状态。
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

	newestRS := newestReplicaSet(rsList, d)
	if newestRS != nil {
		c.logf("定位本次发布 new_rs=%s (revision=%d)", newestRS.Name, rsRevision(newestRS))
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

// newestReplicaSet 返回 Deployment 控制的 revision 最大的 ReplicaSet（本次发布的新 RS）；
// 不存在时返回 nil。revision 取 deployment.kubernetes.io/revision annotation。
func newestReplicaSet(rsList *appsv1.ReplicaSetList, d *appsv1.Deployment) *appsv1.ReplicaSet {
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
	return newestRS
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

// waitForTargetPods 已删除：等待目标 Pod 出现的逻辑并入 locateTargetPods（pod-ready-timeout 窗口轮询）。

// watchPods 阶段2+3：为每个 Pod 启动独立 goroutine，全部登记到全局 wg。
// fullCheck=true（首发 Pod）：阶段2+3 完整检查；false（后置发现补录的 Pod）：仅阶段2
// 就绪检查，不启用阶段3 日志观察/退出检测。
func (c *Checker) watchPods(pods []corev1.Pod, fullCheck bool) {
	for i := range pods {
		p := pods[i]
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.trackPod(p, fullCheck)
		}()
	}
}

// discoverLatePods 后置 Pod 发现器（单 goroutine，登记进全局 wg）：
// 滚动更新中受 maxSurge/maxUnavailable 限制，新 RS 的 Pod 逐个创建——目标定位只要等到
// 第 1 个 Pod 即返回首发名单，后续新 Pod 由本发现器在 pod-ready-timeout 窗口内 1s 轮询
// listTargetPods 补录：出现一个跟踪一个（trackPod fullCheck=false，仅就绪检查，
// 不启用阶段3 日志观察/退出检测——首发已承担"本次发布内容是否异常"判定，后置只验证收敛）。
//
// 期望副本数使用定位时刻的快照（发布过程中 scale 扩容/缩容不属于本工具职责，
// 不做动态跟踪）；补录上限为期望副本数（maxSurge 期间瞬时超额的 surge 副本不纳入，
// DeletionTimestamp 防护本来兜底）。
//
// 终止条件（三选一先到先停）：
//   - 收敛：已跟踪数（首发+后置）达到期望副本数 -> 停止补录
//   - 窗口超时：pod-ready-timeout 到期仍未凑齐 -> 静默退出，不告警不置失败
//     （本工具不校验滚动收敛，"新 Pod 迟迟不出"属 Deployment 滚动状态，由 rollout status 负责）
//   - 全局中断：Interrupt cancelAll 取消窗口上下文 -> 立即退出
//
// tracked 仅由本 goroutine 读写（主 goroutine 在启动本发现器前完成首发写入），
// 无并发竞争；补录的 trackPod goroutine 通过 wg 登记进全局等待集合。
func (c *Checker) discoverLatePods(replicas int32, tracked map[string]struct{}) {
	defer c.wg.Done()
	// 窗口上下文派生 Background（与 trackPod 的 podCtx 同模式）：中断统一走 cancels 登记取消
	discoverCtx, cancelDiscover := context.WithTimeout(context.Background(), c.opts.PodReadyTimeout())
	defer cancelDiscover()
	c.registerCancel(cancelDiscover)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-discoverCtx.Done():
			if len(tracked) < int(replicas) {
				c.logf("后置发现: 窗口结束，已跟踪 %d/%d 个目标 Pod（滚动未凑齐副本数不构成失败，不校验滚动收敛）",
					len(tracked), replicas)
			}
			return
		case <-ticker.C:
			pods, err := c.listTargetPods(discoverCtx)
			if err != nil {
				continue
			}
			for _, p := range pods {
				if _, ok := tracked[p.Name]; ok {
					continue
				}
				tracked[p.Name] = struct{}{}
				c.logf("后置发现: 新目标 Pod %s 出现（phase=%s），启动就绪检查（不启用日志观察）",
					p.Name, p.Status.Phase)
				c.wg.Add(1)
				go func(p corev1.Pod) {
					defer c.wg.Done()
					c.trackPod(p, false)
				}(p)
				if len(tracked) >= int(replicas) {
					c.logf("后置发现: 已跟踪 %d 个目标 Pod（达到期望副本数），停止补录", len(tracked))
					return
				}
			}
		}
	}
}

// trackPod 单个 Pod：阶段2（等待就绪 Ready，独立 pod_ready_timeout）与阶段3
// （日志观察，独立 log_check_timeout，从首次 Running 时刻计时）并行运行。
// fullCheck=true（首发 Pod）：阶段2+3 完整检查；false（后置发现补录的 Pod）：
// 仅阶段2 就绪检查，不启动阶段3（后置副本崩溃由阶段2 就绪超时兜底，无秒级退出检测
// 与日志落盘——首发已承担"本次发布内容是否异常"判定）。
// 有就绪探针的 Pod 在 0/1 Running 期间（Running 但未 Ready）两阶段共同作用：
// 阶段2 继续等 Ready，阶段3 已开始跟踪日志与容器退出。
// 任一路径到达终态即通过 podCtx 联动取消另一路：
//   - 阶段2 超时未就绪 / Pod 被 controller 删除 -> 取消阶段3
//   - 阶段3 容器异常退出 / 重启超限（已告警）-> 取消阶段2 的 Ready 等待
//   - 阶段2 Ready 通过 -> 阶段3 继续至日志窗口自然结束（不取消）
func (c *Checker) trackPod(pod corev1.Pod, fullCheck bool) {
	// Pod 级上下文：阶段2/3 共享，终态联动取消；全局中断时统一取消
	podCtx, podCancel := context.WithCancel(context.Background())
	c.registerCancel(podCancel)

	// 阶段3 终态信号：日志窗口自然到期 / 容器退出等告警终态 / 被 podCancel 联动取消，
	// 均会 close(stage3Done)。
	// trackPod 返回前必须等阶段3 终态：就绪路径阶段3 仍在日志窗口内继续观察，
	// 此时执行 podCancel 会误杀观察（podCtx 防泄漏的释放点因此放在其后）
	stage3Done := make(chan struct{})
	stage3 := false
	defer func() {
		if stage3 {
			<-stage3Done // 等阶段3 终态（上方显式 podCancel 的分支会被加速结束）
		}
		podCancel() // 双方均终态后释放 podCtx（防泄漏；CancelFunc 幂等）
	}()

	// ---- 阶段2：等待 Pod 就绪（每个 Pod 独立超时，非全局共享）----
	c.logf("阶段2: 等待 Pod %s 就绪（pod-ready-timeout=%d 秒）", pod.Name, c.opts.PodReadyTimeoutSec)
	readyCtx, cancelReady := context.WithTimeout(podCtx, c.opts.PodReadyTimeout())
	defer cancelReady()

	notReadyFailure := func() {
		// pod_ready_timeout 内未就绪：置 global_has_pod_failure = true，发送告警并结束
		// （后置 Pod 与首发同权重：Ready 即其全部判据，不 Ready = 发布异常）
		c.logf("阶段2: Pod %s 未就绪（超时 %d 秒），置 pod failure", pod.Name, c.opts.PodReadyTimeoutSec)
		c.setPodFailure()
		detail := fmt.Sprintf("Pod %s 命名空间 %s 未就绪（Ready 超时 %d 秒）",
			pod.Name, pod.Namespace, c.opts.PodReadyTimeoutSec)
		if !fullCheck {
			detail += "（后置副本，仅就绪检查）"
		}
		c.alert(feishu.EventPodStatus, pod.Name, detail)
	}

	// 前半：等待 Pod 进入 Running（Pending/Init 阶段也占用 pod_ready_timeout 预算）
	runningAt, removed := c.waitFirstRunning(readyCtx, pod)
	if removed {
		// Pod 已被 controller 删除（surge 缩容/回滚等）：非应用故障，静默跳过
		return
	}
	if runningAt.IsZero() {
		if readyCtx.Err() == context.Canceled {
			// 被中断/取消：静默返回，由中断流程统一告警
			return
		}
		notReadyFailure()
		return
	}

	// ---- 阶段3：首次 Running 即启动（与阶段2 的 Ready 等待并行）----
	// fullCheck=false（后置发现补录的 Pod）不启动阶段3：仅验证滚动收敛（Ready）
	if fullCheck && c.opts.LogEnable && c.opts.LogCheckTimeoutSec > 0 {
		stage3 = true
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			defer close(stage3Done)
			// 注意：此处不 defer podCancel——日志窗口自然到期不取消阶段2 的
			// Ready 等待（podCtx 仅在容器退出/重启超限告警时由 watchPodLog 内部取消）
			c.watchPodLog(podCtx, podCancel, pod, runningAt)
		}()
	}

	// 后半：等待 Ready（探针校验；无探针容器通常立即 Ready）
	ok, removed := c.waitReady(readyCtx, pod)
	if removed {
		podCancel() // Pod 被删除：联动终止阶段3
		return
	}
	if !ok {
		if readyCtx.Err() == context.Canceled {
			// podCtx 被取消：阶段3 已告警（容器退出/重启超限）或全局中断 -> 静默返回
			return
		}
		podCancel() // 超时未就绪：联动终止阶段3（该故障仅此一条告警）
		notReadyFailure()
		return
	}
	// 就绪：阶段2 结束（耗时日志由 waitReady 打印）；defer 等阶段3 日志窗口自然结束后返回
}

// waitFirstRunning 阶段2 前半：在 ctx 有效期内轮询，等待 Pod 首次进入 Running。
// 返回 (首次观察到 Running 的时刻, 是否被 controller 删除)；
// runningAt 为零值且 removed=false 表示超时/取消（由调用方以 ctx.Err() 区分）。
// 每次重新 Get 最新状态（不信任初始快照）。
func (c *Checker) waitFirstRunning(ctx context.Context, pod corev1.Pod) (time.Time, bool) {
	start := time.Now()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		p, err := c.clientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			c.logf("阶段2: Pod %s 已被删除（controller 缩容/回滚），跳过检查", pod.Name)
			return time.Time{}, true
		}
		if err == nil && p.DeletionTimestamp != nil {
			c.logf("阶段2: Pod %s 正在删除（controller 缩容/回滚），跳过检查", pod.Name)
			return time.Time{}, true
		}
		if err == nil && p.Status.Phase == corev1.PodRunning {
			runningAt := time.Now()
			c.logf("阶段2: Pod %s 已进入 Running（耗时 %.1f 秒）", pod.Name, time.Since(start).Seconds())
			return runningAt, false
		}
		// verbose：每 5 秒打印一次当前状态，便于观察 Pod 卡在哪个状态
		if err == nil && int(time.Since(start).Seconds())%5 == 0 && p.Status.Phase != "" {
			c.logf("阶段2: Pod %s 当前 phase=%s ready=%v（已等待 %.1f 秒）",
				pod.Name, p.Status.Phase, podReady(p), time.Since(start).Seconds())
		}
		select {
		case <-ctx.Done():
			return time.Time{}, false
		case <-ticker.C:
		}
	}
}

// waitReady 阶段2 后半：Pod 已 Running，在 ctx 剩余预算内轮询等待就绪
// （PodReady 条件为 True，含就绪探针校验；无探针的容器 Running 即 Ready）。
// 返回 (是否就绪, 是否被 controller 删除)；ctx 取消（联动/中断）返回 (false, false)。
func (c *Checker) waitReady(ctx context.Context, pod corev1.Pod) (bool, bool) {
	start := time.Now()
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		p, err := c.clientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			c.logf("阶段2: Pod %s 已被删除（controller 缩容/回滚），跳过检查", pod.Name)
			return false, true
		}
		if err == nil && p.DeletionTimestamp != nil {
			c.logf("阶段2: Pod %s 正在删除（controller 缩容/回滚），跳过检查", pod.Name)
			return false, true
		}
		if err == nil && p.Status.Phase == corev1.PodRunning && podReady(p) {
			c.logf("阶段2: Pod %s 已就绪（Running 后耗时 %.1f 秒）", pod.Name, time.Since(start).Seconds())
			return true, false
		}
		// verbose：每 5 秒打印一次等待进度（0/1 Running 探针未过等场景），便于观察卡点
		if err == nil && p.Status.Phase != "" && int(time.Since(start).Seconds())%5 == 0 {
			c.logf("阶段2: Pod %s 等待就绪中 phase=%s ready=%v（已等待 %.1f 秒）",
				pod.Name, p.Status.Phase, podReady(p), time.Since(start).Seconds())
		}
		select {
		case <-ctx.Done():
			return false, false
		case <-ticker.C:
		}
	}
}

// podReady Pod 就绪判定（等价 podutil.IsPodReady 语义，不引入 k8s.io/kubectl）：
// Status.Conditions 中 Type == PodReady 且 Status == ConditionTrue。
// 无就绪探针的容器进入 Running 后 kubelet 即上报 PodReady=True。
func podReady(p *corev1.Pod) bool {
	for _, cond := range p.Status.Conditions {
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// watchPodLog 阶段3：在 log_check_timeout 内以 follow 模式采集该 Pod 容器日志，
// 增量写入本地日志文件。由 trackPod 在 Pod 首次进入 Running 时启动（与阶段2 的
// Ready 等待并行；有探针时 0/1 Running 期间两阶段共同作用）。
// 两条独立路径并行：
//
//	① 日志流：rec.TrackLog 持续增量处理——未启用 LLM 时命中关键字即落盘；
//	  启用 LLM 时命中行经仲裁，存在真错误才以容器启动时刻为起点全量拉取落盘
//	② 容器状态：周期 Get Pod，容器 Terminated 且退出码 != 0 -> global_has_pod_failure
//
// 结束时机：
//   - ② 命中 -> 停止日志采集（cancelLog），等待剩余缓冲日志全部落盘，发送异常告警，
//     并 podCancel 终止阶段2 的 Ready 等待（该故障以本条告警为准）
//   - 日志观察窗口超时 -> 若存在真错误，置 global_has_warning
//   - podCtx 被取消（阶段2 超时告警/中断/Pod 删除）-> 静默结束，不置 warning
func (c *Checker) watchPodLog(ctx context.Context, podCancel context.CancelFunc, pod corev1.Pod, runningAt time.Time) {
	if c.opts.LogCheckTimeoutSec <= 0 {
		return
	}
	// 计时起点 = Pod 变为 Running 的时刻（每 Pod 独立超时上下文，派生 podCtx 联动取消）
	remaining := c.opts.LogCheckTimeout() - time.Since(runningAt)
	if remaining < 0 {
		remaining = 0
	}
	logCtx, cancelLog := context.WithTimeout(ctx, remaining)
	defer cancelLog()
	// 开始日志即标注日志内容判定是否开启（parseArgs 折算后 LogErrKeywords 为空
	// 即判定关闭——errorHit 恒 false，窗口结束不会报"未发现真错误"），
	// 避免"未开启判定却说未发现真错误"的误导
	kwState := "错误关键字判定关闭"
	if len(c.opts.LogErrKeywords) > 0 {
		kwState = "错误关键字判定开启"
	}
	c.logf("阶段3: 开始观察 Pod %s 日志（log-check-timeout=%.0f 秒，%s）", pod.Name, remaining.Seconds(), kwState)

	var (
		mu                  sync.Mutex
		podLocalHasErrorLog bool
	)
	logDone := make(chan struct{})
	go func() {
		defer close(logDone)
		_, hit, err := c.rec.TrackLog(logCtx, c.clientset, &pod, c.judge,
			c.opts.LogErrKeywords, c.opts.LogIgnoreKeywords, c.opts.LogTail)
		if err != nil && c.opts.Verbose {
			fmt.Println(console.Yellow(fmt.Sprintf("[WARN] 跟踪 %s/%s 日志出错: %v", pod.Namespace, pod.Name, err)))
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
			if p.DeletionTimestamp != nil {
				// controller 删除中的 Pod（surge 缩容/回滚）：退出属预期行为，不计异常
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
			// ② pod 容器异常退出（或重启超限，exitCh 哨兵 code=-1）：
			// 停止日志采集，把剩余缓冲区日志全部写入日志文件落盘
			if code < 0 {
				c.logf("阶段3: Pod %s 重启次数超过上限（--max-restart=%d），停止日志观察", pod.Name, c.opts.MaxRestart)
			} else {
				c.logf("阶段3: Pod %s 容器异常退出（code=%d），停止日志观察", pod.Name, code)
			}
			cancelLog()
			c.waitLogFlush(logDone)
			c.setPodFailure()
			c.alertPodExit(pod, code)
			podCancel() // 终止阶段2 的 Ready 等待（0/1 Running 期间崩溃场景，仅此一条告警）
			return
		case <-logCtx.Done():
			// ① 日志观察窗口超时结束
			c.waitLogFlush(logDone)
			if logCtx.Err() == context.Canceled {
				// podCtx 被取消（阶段2 未就绪告警 / 全局中断 / Pod 删除联动）：
				// 不置 warning，静默结束，由对应流程统一告警
				return
			}
			mu.Lock()
			hit := podLocalHasErrorLog
			mu.Unlock()
			if hit {
				// 存在真错误日志（LLM 确认或降级命中）但 pod 存活 -> global_has_warning = true
				c.logf("阶段3: Pod %s 存在真错误日志，置 warning", pod.Name)
				c.setWarning()
			} else if len(c.opts.LogErrKeywords) == 0 {
				// 判定未开启（--keyword-check=false 或关键字列表为空）：errorHit 恒 false，
				// 不能说"未发现真错误"——本窗口仅做了日志透传（--log-console）与退出检测
				c.logf("阶段3: Pod %s 日志观察窗口结束（错误关键字判定未开启，未做内容判定）", pod.Name)
			} else {
				c.logf("阶段3: Pod %s 日志观察窗口结束，未发现真错误", pod.Name)
			}
			return
		}
	}
}

// waitLogFlush 等待日志跟踪 goroutine 完成文件落盘（带兜底超时，避免异常卡死）。
// LLM 仲裁启用时需覆盖在途判定（≤2×llm-timeout）+ 全量快照拉取的耗时。
func (c *Checker) waitLogFlush(logDone chan struct{}) {
	cap := 5 * time.Second
	if c.judge != nil && c.judge.Enabled() {
		cap = 2*c.opts.LLMTimeout() + 15*time.Second
	}
	select {
	case <-logDone:
	case <-time.After(cap):
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
	msg := fmt.Sprintf("Pod %s 命名空间 %s 容器异常退出（退出码 %d）",
		pod.Name, pod.Namespace, code)
	if path := c.rec.RecordedPath(pod.Namespace, pod.Name); path != "" {
		msg += fmt.Sprintf("，日志已落盘: %s", path)
	}
	c.alert(feishu.EventContainerExit, pod.Name, msg)
}

// Interrupt 收到中断信号（SIGINT/SIGTERM）时调用：
// 调用所有 cancel 函数（全部 cancel_pod_ready、全部 cancel_pod_log），
// wg.Wait() 等待正在写日志的 goroutine 完成文件落盘，组装中断告警发送（飞书/降级控制台）。
// 注意：目标定位为单次同步查询（无轮询、无超时上下文）——中断时由信号处理流程直接退出。
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

// cancelAll 调用全部已登记的 cancel（全部 cancel_pod_ready、全部 cancel_pod_log）
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

// alert 发送告警：统一收敛到 feishu 层，保证控制台输出与飞书卡片内容一致：
//
//	webhook 已配置：发送 interactive 卡片（事件类型标题 + 资源名 + 详情 + 时间）；
//	              发送失败（网络/非200）降级为控制台打印，告警不丢失
//	webhook 未配置：降级为控制台打印
//
// 控制台输出（两种降级路径）与卡片正文同为 event/key/detail 三要素；
// 均经去重窗口（同事件+同资源在 dedup-window 内仅一条，避免刷屏）。
func (c *Checker) alert(event string, key, detail string) {
	c.feishu.Send(event, key, detail)
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
