package checker

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"

	"kubecheck/internal/feishu"
	"kubecheck/internal/options"
	"kubecheck/internal/recorder"
)

// captureStdout 重定向标准输出，返回恢复函数与捕获内容
func captureStdout(t *testing.T) func() string {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stdout = w
	return func() string {
		w.Close()
		var buf bytes.Buffer
		io.Copy(&buf, r)
		os.Stdout = old
		return buf.String()
	}
}

func newFakeChecker(t *testing.T, objs ...runtime.Object) (*Checker, *fake.Clientset) {
	opts := &options.Options{
		Namespace:          "default",
		ResourceName:       "app",
		PodReadyTimeoutSec: 1,
		LogCheckTimeoutSec: 1,
		LogErrorDir:        tempLogDir(t),
		LogErrKeywords:     []string{"error", "panic"},
	}
	cs := fake.NewSimpleClientset(objs...)
	return &Checker{
		opts:      opts,
		clientset: cs,
		feishu:    feishu.New("", "", 0),
		rec:       recorder.New(opts.LogErrorDir, true, false),
	}, cs
}

// makeDeployment 构造测试用 Deployment（selector app=app，单副本）。
// status 字段不再参与目标定位判定（不校验 Deployment 状态），仅为历史用例兼容保留。
func makeDeployment(ready bool) *appsv1.Deployment {
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "default", Generation: 1},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "app"}},
			Replicas: int32Ptr(1),
		},
		Status: appsv1.DeploymentStatus{
			Replicas:           1,
			UpdatedReplicas:    0,
			ReadyReplicas:      0,
			AvailableReplicas:  1,
			ObservedGeneration: 1,
		},
	}
	if ready {
		d.Status.UpdatedReplicas = 1
		d.Status.ReadyReplicas = 1
	}
	return d
}

func makeReadyPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: map[string]string{"app": "app"}},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionTrue},
			},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "c", Ready: true}},
		},
	}
}

func makeUnreadyPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: map[string]string{"app": "app"}},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{
				{Type: corev1.PodReady, Status: corev1.ConditionFalse},
			},
			ContainerStatuses: []corev1.ContainerStatus{{Name: "c", Ready: false}},
		},
	}
}

// makeExitedPod 容器处于 Terminated 且退出码 != 0 的 Pod（阶段3 异常退出场景）
func makeExitedPod(name string, code int32) *corev1.Pod {
	p := makeReadyPod(name)
	p.Status.ContainerStatuses[0].State = corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: code},
	}
	return p
}

func int32Ptr(v int32) *int32 { return &v }

// TestListTargetPodsImmediate 目标定位：存在目标 Pod 时单次查询立即返回（无轮询/超时）
func TestListTargetPodsImmediate(t *testing.T) {
	c, _ := newFakeChecker(t, makeDeployment(true), makeReadyPod("p1"))
	start := time.Now()
	pods, err := c.listTargetPods(context.Background())
	if err != nil {
		t.Fatalf("listTargetPods: %v", err)
	}
	if len(pods) != 1 || pods[0].Name != "p1" {
		t.Fatalf("got pods %v, want [p1]", pods)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("locate should be immediate, took %v", time.Since(start))
	}
}

// TestListTargetPodsMidRollout 滚动进行中：仍以最新 revision RS 的 selector 定位新 Pod；
// 旧 RS 的 Ready Pod 不得被纳入（误锁定防护：selector 含 pod-template-hash）
func TestListTargetPodsMidRollout(t *testing.T) {
	d := makeDeployment(false)
	d.UID = "deploy-uid"
	d.Spec.Replicas = int32Ptr(2)
	d.Status.Replicas = 3 // 旧 1 + 新 2，surge 进行中
	d.Status.ObservedGeneration = 1

	mkRS := func(name, rev, hash string) *appsv1.ReplicaSet {
		return &appsv1.ReplicaSet{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: "default",
				Annotations: map[string]string{
					"deployment.kubernetes.io/revision": rev,
				},
				OwnerReferences: []metav1.OwnerReference{{
					Kind: "Deployment", Name: d.Name, UID: d.UID,
				}},
			},
			Spec: appsv1.ReplicaSetSpec{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "app", "pod-template-hash": hash}},
			},
		}
	}
	oldRS := mkRS("app-old", "1", "old")
	newRS := mkRS("app-new", "2", "new")

	oldPod := makeReadyPod("old-ready")
	oldPod.Labels = map[string]string{"app": "app", "pod-template-hash": "old"}
	newPending := makeUnreadyPod("new-pending") // Pending/拉镜像中也计入名单
	newPending.Labels = map[string]string{"app": "app", "pod-template-hash": "new"}
	newRunning := makeReadyPod("new-running")
	newRunning.Labels = map[string]string{"app": "app", "pod-template-hash": "new"}

	c, _ := newFakeChecker(t, d, oldRS, newRS, oldPod, newPending, newRunning)
	pods, err := c.listTargetPods(context.Background())
	if err != nil {
		t.Fatalf("listTargetPods: %v", err)
	}
	if len(pods) != 2 {
		t.Fatalf("got %d pods, want 2 (new RS pods)", len(pods))
	}
	for _, p := range pods {
		if p.Labels["pod-template-hash"] != "new" {
			t.Fatalf("locked non-new-RS pod: %s", p.Name)
		}
	}
}

// TestTrackPodReadyTimeoutSetsFailure 阶段2 超时语义：
// Pod 未就绪（Ready 条件未满足）-> 置 global_has_pod_failure = true，发送告警并结束该 goroutine。
func TestTrackPodReadyTimeoutSetsFailure(t *testing.T) {
	c, cs := newFakeChecker(t, makeUnreadyPod("p1"))
	_ = cs
	restore := captureStdout(t)
	c.trackPod(*makeUnreadyPod("p1"), true)
	out := restore()
	if !bytes.Contains([]byte(out), []byte("未就绪")) {
		t.Fatalf("expected not-ready alert, got: %q", out)
	}
	if !c.hasPodFailure {
		t.Fatal("pod not ready within timeout should set hasPodFailure")
	}
	if c.hasWarning {
		t.Fatal("pod not ready should NOT set hasWarning")
	}
}

// TestTrackPodRemovedSkipsFailure 阶段2 防误报：
// Pod 被 controller 删除（DeletionTimestamp 非空，surge 缩容/回滚）-> 非应用故障，
// 不置 failure、不告警
func TestTrackPodRemovedSkipsFailure(t *testing.T) {
	now := metav1.Now()
	p := makeReadyPod("p1")
	p.DeletionTimestamp = &now
	c, _ := newFakeChecker(t, p)
	restore := captureStdout(t)
	c.trackPod(*p, true)
	out := restore()
	if c.hasPodFailure {
		t.Fatal("controller-removed pod should NOT set hasPodFailure")
	}
	if c.hasWarning {
		t.Fatal("controller-removed pod should NOT set hasWarning")
	}
	if bytes.Contains([]byte(out), []byte("[ALERT]")) {
		t.Fatalf("controller-removed pod should not alert, got: %q", out)
	}
}

// TestTrackPodContainerExitSetsFailure 阶段3 语义：
// 容器异常退出（退出码 != 0）-> global_has_pod_failure = true，并发送异常告警。
func TestTrackPodContainerExitSetsFailure(t *testing.T) {
	c, cs := newFakeChecker(t, makeExitedPod("p1", 1))
	_ = cs
	restore := captureStdout(t)
	c.watchPodLog(context.Background(), func() {}, *makeExitedPod("p1", 1), time.Now())
	out := restore()
	if !c.hasPodFailure {
		t.Fatal("container exit code != 0 should set hasPodFailure")
	}
	if !bytes.Contains([]byte(out), []byte("异常退出")) {
		t.Fatalf("expected container-exit alert, got: %q", out)
	}
}

// TestTrackPodLogWindowEndsBeforeReadyTimeout 防回归（e2e C2 实测发现）：
// 阶段3 日志窗口（log-check-timeout=5s）早于阶段2 就绪超时（pod-ready-timeout=15s）
// 自然到期时，不得取消阶段2 的 Ready 等待——探针失败的 Pod 应继续等满超时
// 并告警「未就绪」（exit 3），而不是被误杀后静默通过（exit 0）
func TestTrackPodLogWindowEndsBeforeReadyTimeout(t *testing.T) {
	// 探针失败：phase=Running 但 PodReady=False，容器存活
	p := makeUnreadyPod("p1")
	p.Status.Phase = corev1.PodRunning
	c, _ := newFakeChecker(t, p)
	c.opts.CheckPodStatus = true
	c.opts.LogEnable = true
	c.opts.PodReadyTimeoutSec = 2 // 阶段2 预算
	c.opts.LogCheckTimeoutSec = 1 // 阶段3 窗口更短（先自然到期）

	start := time.Now()
	restore := captureStdout(t)
	c.trackPod(*p, true)
	c.wg.Wait() // 等阶段3 goroutine 完成
	out := restore()
	elapsed := time.Since(start)

	if !c.hasPodFailure {
		t.Fatal("log window expiry must not kill stage-2 readiness wait; expected not-ready failure")
	}
	if !bytes.Contains([]byte(out), []byte("未就绪")) {
		t.Fatalf("expected not-ready alert after stage-3 window ended, got: %q", out)
	}
	if elapsed < 1500*time.Millisecond {
		t.Fatalf("stage-2 should wait its full timeout (~2s), returned in %v", elapsed)
	}
}
func TestTrackPodCrashBeforeReady(t *testing.T) {
	// 构造：phase=Running、PodReady=False（探针未过）、容器 Terminated exit=1
	p := makeUnreadyPod("p1")
	p.Status.Phase = corev1.PodRunning
	p.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name: "main",
		State: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Reason: "Error"},
		},
		Ready: false,
	}}
	c, _ := newFakeChecker(t, p)
	c.opts.CheckPodStatus = true
	c.opts.LogEnable = true // 启用阶段3（与阶段2 并行）
	restore := captureStdout(t)
	c.trackPod(*p, true)
	c.wg.Wait() // 等阶段3 goroutine 完成
	out := restore()
	if !c.hasPodFailure {
		t.Fatal("crashed container should set hasPodFailure")
	}
	if !bytes.Contains([]byte(out), []byte("容器异常退出")) {
		t.Fatalf("expected container-exit alert, got: %q", out)
	}
	if bytes.Contains([]byte(out), []byte("未就绪")) {
		t.Fatalf("crash-before-ready should not emit duplicate not-ready alert, got: %q", out)
	}
}

// TestWatchPodsAllReadyNoFailure 阶段2+3 正常路径：
// Pod 已 Running 且无容器退出，聚合后无 failure、无 warning。
func TestWatchPodsAllReadyNoFailure(t *testing.T) {
	c, _ := newFakeChecker(t, makeReadyPod("p1"))
	c.opts.LogEnable = false
	c.watchPods([]corev1.Pod{*makeReadyPod("p1")}, true)
	c.wg.Wait()
	if c.hasPodFailure {
		t.Fatal("healthy pod should not set hasPodFailure")
	}
	if c.hasWarning {
		t.Fatal("healthy pod should not set hasWarning")
	}
}

// TestRunNoPods replicas=0：无目标可等 -> 立即告警"未找到目标 Pod"并以 code=2 退出（不轮询）
func TestRunNoPods(t *testing.T) {
	d := makeDeployment(true)
	d.Spec.Replicas = int32Ptr(0) // replicas=0
	c, _ := newFakeChecker(t, d)
	c.opts.CheckPodStatus = true
	start := time.Now()
	restore := captureStdout(t)
	code := c.Run(context.Background())
	out := restore()
	if code != CodeNoTargetPod {
		t.Fatalf("no pods -> code = %d, want %d", code, CodeNoTargetPod)
	}
	if !bytes.Contains([]byte(out), []byte("未找到目标 Pod")) {
		t.Fatalf("expected no-target-pod alert, got: %q", out)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatalf("replicas=0 should exit immediately, took %v", time.Since(start))
	}
}

// TestRunNoPodsNoRS replicas>0 但窗口内始终无 RS/无 Pod（如发布尚未触发新 RS）->
// 轮询等待至 pod-ready-timeout 超时后告警并退出 code=2（目标定位不校验 Deployment 状态，
// 只等待目标 Pod 出现）
func TestRunNoPodsNoRS(t *testing.T) {
	c, _ := newFakeChecker(t, makeDeployment(false)) // replicas=1，无 RS 无 Pod
	c.opts.PodReadyTimeoutSec = 1
	c.opts.CheckPodStatus = true
	start := time.Now()
	restore := captureStdout(t)
	code := c.Run(context.Background())
	out := restore()
	elapsed := time.Since(start)
	if code != CodeNoTargetPod {
		t.Fatalf("no rs/pods -> code = %d, want %d", code, CodeNoTargetPod)
	}
	if !bytes.Contains([]byte(out), []byte("未找到目标 Pod")) {
		t.Fatalf("expected no-target-pod alert, got: %q", out)
	}
	if elapsed < 900*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("waited timeout elapsed = %v, want ~1s", elapsed)
	}
}

// TestRunWaitsForTargetPod 竞态窗口：发布刚触发（新 RS 已创建但 Pod 尚未出现）时启动工具 ->
// 目标定位在 pod-ready-timeout 窗口内轮询等待，目标 Pod 出现后正常定位（不再立即误判退出）
func TestRunWaitsForTargetPod(t *testing.T) {
	c, cs := newFakeChecker(t, makeDeployment(true)) // replicas=1，启动时无 Pod
	c.opts.PodReadyTimeoutSec = 10
	c.opts.CheckPodStatus = false // 定位成功即退出，聚焦目标定位语义
	go func() {
		time.Sleep(1500 * time.Millisecond) // 模拟 1.5s 后新 RS 的 Pod 才被创建
		if err := cs.Tracker().Add(makeReadyPod("p1")); err != nil {
			t.Errorf("add pod: %v", err)
		}
	}()
	start := time.Now()
	restore := captureStdout(t)
	code := c.Run(context.Background())
	restore()
	if code != CodeOK {
		t.Fatalf("late-arriving pod should be located, code = %d, want %d", code, CodeOK)
	}
	if time.Since(start) < time.Second {
		t.Fatal("should have waited for target pod to appear, returned too fast")
	}
}

// TestListTargetPodsSkipsTerminating 阶段2 前置：
// 正在删除（DeletionTimestamp 非空）但 phase 仍为 Running 的旧 Pod，
// 不得被当作本次发布的有效目标，否则阶段2 会误判健康、漏检真正异常的 Pod。
func TestListTargetPodsSkipsTerminating(t *testing.T) {
	now := metav1.Now()
	d := makeDeployment(true)
	d.UID = "deploy-uid"
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "app-abc",
			Namespace: "default",
			Annotations: map[string]string{
				"deployment.kubernetes.io/revision": "2",
			},
			OwnerReferences: []metav1.OwnerReference{{
				Kind: "Deployment", Name: d.Name, UID: d.UID,
			}},
		},
		Spec: appsv1.ReplicaSetSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "app", "pod-template-hash": "abc"}},
		},
	}
	// 旧 Pod：Terminating 中，phase 仍是 Running（应被过滤）
	terminating := makeReadyPod("old-terminating")
	terminating.DeletionTimestamp = &now
	terminating.Labels = map[string]string{"app": "app", "pod-template-hash": "abc"}
	// 新 Pod：Pending（应保留，等待阶段2 检查）
	pending := makeUnreadyPod("new-pending")
	pending.Labels = map[string]string{"app": "app", "pod-template-hash": "abc"}

	c, _ := newFakeChecker(t, d, rs, terminating, pending)
	pods, err := c.listTargetPods(context.Background())
	if err != nil {
		t.Fatalf("listTargetPods: %v", err)
	}
	if len(pods) != 1 {
		t.Fatalf("got %d pods, want 1 (only new-pending)", len(pods))
	}
	if pods[0].Name != "new-pending" {
		t.Fatalf("got pod %s, want new-pending", pods[0].Name)
	}
}

func TestAlertFallbackConsole(t *testing.T) {
	c, _ := newFakeChecker(nil)
	restore := captureStdout(t)
	c.alert(feishu.EventNoTargetPod, "app", "no target pod")
	out := restore()
	if !bytes.Contains([]byte(out), []byte("[ALERT]")) {
		t.Fatalf("console fallback expected, got: %q", out)
	}
}

func tempLogDir(t *testing.T) string {
	if t != nil {
		return t.TempDir()
	}
	d, _ := os.MkdirTemp("", "flow-check-")
	return d
}

// ---------- 后置发现（多副本滚动场景）----------

// TestTrackPodLiteSkipsStage3 fullCheck=false（后置补录 Pod）：即使 --log-enable
// 开启也不启动阶段3（无日志观察/退出检测），仅阶段2 就绪检查，Ready 即通过
func TestTrackPodLiteSkipsStage3(t *testing.T) {
	p := makeReadyPod("late-1")
	c, _ := newFakeChecker(t, p)
	c.opts.CheckPodStatus = true
	c.opts.LogEnable = true
	c.opts.Verbose = true
	restore := captureStdout(t)
	c.trackPod(*p, false)
	c.wg.Wait()
	out := restore()
	if c.hasPodFailure || c.hasWarning {
		t.Fatalf("ready-only pod should pass cleanly, failure=%v warning=%v", c.hasPodFailure, c.hasWarning)
	}
	if bytes.Contains([]byte(out), []byte("阶段3")) {
		t.Fatalf("fullCheck=false must not start stage 3, got: %q", out)
	}
}

// TestTrackPodFullCheckStartsStage3 对照组：fullCheck=true（首发 Pod）正常启动阶段3
func TestTrackPodFullCheckStartsStage3(t *testing.T) {
	p := makeReadyPod("first-1")
	c, _ := newFakeChecker(t, p)
	c.opts.CheckPodStatus = true
	c.opts.LogEnable = true
	c.opts.Verbose = true
	restore := captureStdout(t)
	c.trackPod(*p, true)
	c.wg.Wait()
	out := restore()
	if !bytes.Contains([]byte(out), []byte("阶段3")) {
		t.Fatalf("fullCheck=true should start stage 3, got: %q", out)
	}
}

// TestRunLatePodDiscoveredReadyChecked 多副本滚动：首发 1 个（立即完整检查），
// 后置 Pod 稍后出现 -> 发现器补录（仅就绪检查）-> Ready 通过 -> exit 0
func TestRunLatePodDiscoveredReadyChecked(t *testing.T) {
	d := makeDeployment(true)
	d.Spec.Replicas = int32Ptr(2)
	c, cs := newFakeChecker(t, d, makeReadyPod("p1"))
	c.opts.CheckPodStatus = true
	c.opts.PodReadyTimeoutSec = 5 // 兼作后置发现窗口
	c.opts.Verbose = true
	go func() {
		time.Sleep(1500 * time.Millisecond) // 模拟滚动中第 2 个新 Pod 稍后创建
		if err := cs.Tracker().Add(makeReadyPod("p2")); err != nil {
			t.Errorf("add pod: %v", err)
		}
	}()
	restore := captureStdout(t)
	code := c.Run(context.Background())
	out := restore()
	if code != CodeOK {
		t.Fatalf("late ready pod -> code = %d, want %d", code, CodeOK)
	}
	if !bytes.Contains([]byte(out), []byte("后置发现: 新目标 Pod p2 出现")) {
		t.Fatalf("expected late-pod discovery log, got: %q", out)
	}
}

// TestRunLatePodNotReadySetsFailure 后置 Pod 就绪超时：与首发同权重置 pod failure
// -> exit 3，告警标注"后置副本，仅就绪检查"
func TestRunLatePodNotReadySetsFailure(t *testing.T) {
	d := makeDeployment(true)
	d.Spec.Replicas = int32Ptr(2)
	c, cs := newFakeChecker(t, d, makeReadyPod("p1"))
	c.opts.CheckPodStatus = true
	c.opts.PodReadyTimeoutSec = 3 // 发现窗口 + 后置 Pod 独立就绪预算
	c.opts.Verbose = true
	go func() {
		time.Sleep(1500 * time.Millisecond)
		if err := cs.Tracker().Add(makeUnreadyPod("p2")); err != nil {
			t.Errorf("add pod: %v", err)
		}
	}()
	restore := captureStdout(t)
	code := c.Run(context.Background())
	out := restore()
	if code != CodePodNotReady {
		t.Fatalf("late unready pod -> code = %d, want %d", code, CodePodNotReady)
	}
	if !bytes.Contains([]byte(out), []byte("未就绪")) {
		t.Fatalf("expected not-ready alert, got: %q", out)
	}
	if !bytes.Contains([]byte(out), []byte("后置副本")) {
		t.Fatalf("expected late-pod marker in alert, got: %q", out)
	}
	if c.hasWarning {
		t.Fatal("ready-timeout should not set hasWarning")
	}
}

// TestRunLatePodConvergeStopsAtReplicas 收敛：已跟踪数达到期望副本数即停止补录，
// 之后出现的第 3 个 Pod（surge 场景）不再纳入——即使 Pending 也不影响结果
func TestRunLatePodConvergeStopsAtReplicas(t *testing.T) {
	d := makeDeployment(true)
	d.Spec.Replicas = int32Ptr(2)
	c, cs := newFakeChecker(t, d, makeReadyPod("p1"))
	c.opts.CheckPodStatus = true
	c.opts.PodReadyTimeoutSec = 5
	c.opts.Verbose = true
	go func() {
		time.Sleep(1200 * time.Millisecond) // ~2s tick 补录 p2 -> 收敛停止
		if err := cs.Tracker().Add(makeReadyPod("p2")); err != nil {
			t.Errorf("add p2: %v", err)
		}
		time.Sleep(1200 * time.Millisecond) // 2.4s：发现器已收敛退出
		if err := cs.Tracker().Add(makeUnreadyPod("p3")); err != nil {
			t.Errorf("add p3: %v", err)
		}
	}()
	restore := captureStdout(t)
	code := c.Run(context.Background())
	out := restore()
	if code != CodeOK {
		t.Fatalf("converged pods -> code = %d, want %d", code, CodeOK)
	}
	if !bytes.Contains([]byte(out), []byte("停止补录")) {
		t.Fatalf("expected converge log, got: %q", out)
	}
	if bytes.Contains([]byte(out), []byte("p3")) {
		t.Fatalf("surge pod after converge must not be tracked, got: %q", out)
	}
}

// TestRunLatePodWindowExpirySilent 窗口超时未凑齐副本数（滚动卡住，后置 Pod 不出现）：
// 已出现的 Pod 均 Ready -> 静默 exit 0，不因"少副本"告警或置失败（不校验滚动收敛）
func TestRunLatePodWindowExpirySilent(t *testing.T) {
	d := makeDeployment(true)
	d.Spec.Replicas = int32Ptr(3)
	c, _ := newFakeChecker(t, d, makeReadyPod("p1")) // 仅 1/3，其余永不出现
	c.opts.CheckPodStatus = true
	c.opts.PodReadyTimeoutSec = 1 // 发现窗口 1s，快速到期
	c.opts.Verbose = true
	restore := captureStdout(t)
	code := c.Run(context.Background())
	out := restore()
	if code != CodeOK {
		t.Fatalf("unfilled quorum with all tracked pods ready -> code = %d, want %d", code, CodeOK)
	}
	if bytes.Contains([]byte(out), []byte("[ALERT]")) {
		t.Fatalf("window expiry must be silent (no rolling-convergence check), got: %q", out)
	}
	if !bytes.Contains([]byte(out), []byte("窗口结束")) {
		t.Fatalf("expected window-expiry trace log, got: %q", out)
	}
}

// TestRunFullQuorumNoDiscovery 启动时首发已达期望副本数（滚动完成后再检查）：
// 不启动后置发现器，行为与历史版本一致，快速通过
func TestRunFullQuorumNoDiscovery(t *testing.T) {
	d := makeDeployment(true)
	d.Spec.Replicas = int32Ptr(2)
	c, _ := newFakeChecker(t, d, makeReadyPod("p1"), makeReadyPod("p2"))
	c.opts.CheckPodStatus = true
	c.opts.PodReadyTimeoutSec = 5
	c.opts.Verbose = true
	start := time.Now()
	restore := captureStdout(t)
	code := c.Run(context.Background())
	out := restore()
	if code != CodeOK {
		t.Fatalf("code = %d, want %d", code, CodeOK)
	}
	if bytes.Contains([]byte(out), []byte("后置发现")) {
		t.Fatalf("quorum reached at start must not start discovery, got: %q", out)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("should complete fast without discovery wait, took %v", time.Since(start))
	}
}
