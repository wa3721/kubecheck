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
		Namespace:             "default",
		ResourceName:          "app",
		DeployReadyTimeoutSec: 1,
		PodReadyTimeoutSec:    1,
		LogCheckTimeoutSec:    1,
		LogErrorDir:           tempLogDir(t),
		LogErrKeywords:        []string{"error", "panic"},
	}
	cs := fake.NewSimpleClientset(objs...)
	return &Checker{
		opts:      opts,
		clientset: cs,
		feishu:    feishu.New("", "", 0),
		rec:       recorder.New(opts.LogErrorDir),
	}, cs
}

// makeDeployment 构造满足阶段1判定条件的 Deployment：
// status.updatedReplicas == spec.replicas && status.readyReplicas == spec.replicas
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
			Phase:             corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{Name: "c", Ready: true}},
		},
	}
}

func makeUnreadyPod(name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default", Labels: map[string]string{"app": "app"}},
		Status: corev1.PodStatus{
			Phase:             corev1.PodPending,
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

func TestRolloutComplete(t *testing.T) {
	if !rolloutComplete(makeDeployment(true)) {
		t.Fatal("ready deployment should satisfy rolloutComplete")
	}
	if rolloutComplete(makeDeployment(false)) {
		t.Fatal("unready deployment should not satisfy rolloutComplete")
	}
	// 滚动初始瞬态：spec.replicas=1 但 status 全为 0（controller 尚未更新）-> 不满足
	d := makeDeployment(false)
	d.Status.Replicas = 0
	if rolloutComplete(d) {
		t.Fatal("initial rolling state (status zeroed) must NOT satisfy rolloutComplete")
	}
	// 滚动进行中（真实场景）：spec=1，旧 RS Pod 仍 Ready、新 RS Pod 未就绪。
	// status.replicas=2（旧 RS 未缩容）且 readyReplicas=1（就绪的是旧 Pod），
	// 仅比较 updated/ready 会误判完成 -> 不满足
	d2 := makeDeployment(true)
	d2.Status.Replicas = 2
	d2.Status.UpdatedReplicas = 1
	d2.Status.ReadyReplicas = 1
	if rolloutComplete(d2) {
		t.Fatal("mid-rollout with old pod ready and new pod unready must NOT satisfy rolloutComplete")
	}
	// observedGeneration 落后于 generation（controller 尚未观察到最新 spec）-> 不满足
	d3 := makeDeployment(true)
	d3.Status.ObservedGeneration = 0
	if rolloutComplete(d3) {
		t.Fatal("observedGeneration < generation must NOT satisfy rolloutComplete")
	}
}

func TestWaitDeploymentReadyImmediate(t *testing.T) {
	c, _ := newFakeChecker(t, makeDeployment(true))
	if !c.waitDeploymentReady(context.Background()) {
		t.Fatal("should be ready immediately")
	}
}

func TestWaitDeploymentReadyTimeoutThenAlert(t *testing.T) {
	c, _ := newFakeChecker(t, makeDeployment(false))
	start := time.Now()
	ok := c.waitDeploymentReady(context.Background())
	elapsed := time.Since(start)
	if ok {
		t.Fatal("unready deployment should time out -> false")
	}
	if elapsed < 900*time.Millisecond || elapsed > 3*time.Second {
		t.Fatalf("timeout elapsed = %v, want ~1s", elapsed)
	}
}

// TestTrackPodReadyTimeoutSetsFailure 阶段2 超时语义：
// Pod 未进入 Running -> 置 global_has_pod_failure = true，发送告警并结束该 goroutine。
func TestTrackPodReadyTimeoutSetsFailure(t *testing.T) {
	c, cs := newFakeChecker(t, makeUnreadyPod("p1"))
	_ = cs
	restore := captureStdout(t)
	c.trackPod(context.Background(), *makeUnreadyPod("p1"))
	out := restore()
	if !bytes.Contains([]byte(out), []byte("未进入 running")) {
		t.Fatalf("expected not-running alert, got: %q", out)
	}
	if !c.hasPodFailure {
		t.Fatal("pod not running within timeout should set hasPodFailure")
	}
	if c.hasWarning {
		t.Fatal("pod not running should NOT set hasWarning")
	}
}

// TestTrackPodContainerExitSetsFailure 阶段3 语义：
// 容器异常退出（退出码 != 0）-> global_has_pod_failure = true，并发送异常告警。
func TestTrackPodContainerExitSetsFailure(t *testing.T) {
	c, cs := newFakeChecker(t, makeExitedPod("p1", 1))
	_ = cs
	restore := captureStdout(t)
	c.watchPodLog(context.Background(), *makeExitedPod("p1", 1), time.Now())
	out := restore()
	if !c.hasPodFailure {
		t.Fatal("container exit code != 0 should set hasPodFailure")
	}
	if !bytes.Contains([]byte(out), []byte("异常退出")) {
		t.Fatalf("expected container-exit alert, got: %q", out)
	}
}

// TestWatchPodsAllReadyNoFailure 阶段2+3 正常路径：
// Pod 已 Running 且无容器退出，聚合后无 failure、无 warning。
func TestWatchPodsAllReadyNoFailure(t *testing.T) {
	c, _ := newFakeChecker(t, makeReadyPod("p1"))
	c.opts.LogEnable = false
	c.watchPods(context.Background(), []corev1.Pod{*makeReadyPod("p1")})
	c.wg.Wait()
	if c.hasPodFailure {
		t.Fatal("healthy pod should not set hasPodFailure")
	}
	if c.hasWarning {
		t.Fatal("healthy pod should not set hasWarning")
	}
}

// TestRunNoPods 阶段2 前置：滚动完成后未产生任何 Pod -> 告警并以非 0 退出
func TestRunNoPods(t *testing.T) {
	c, _ := newFakeChecker(t, makeDeployment(true))
	c.opts.CheckPodStatus = true
	restore := captureStdout(t)
	code := c.Run(context.Background())
	out := restore()
	if code != CodePodNotReady {
		t.Fatalf("no pods -> code = %d, want %d", code, CodePodNotReady)
	}
	if !bytes.Contains([]byte(out), []byte("未产生任何 Pod")) {
		t.Fatalf("expected no-pod alert, got: %q", out)
	}
}

// TestRunDeployTimeout 阶段1 超时：Deployment 未滚动完成 -> 告警后直接退出（code=2），不进入后续阶段
func TestRunDeployTimeout(t *testing.T) {
	c, _ := newFakeChecker(t, makeDeployment(false))
	c.opts.CheckPodStatus = true
	restore := captureStdout(t)
	code := c.Run(context.Background())
	out := restore()
	if code != CodeDeployTimeout {
		t.Fatalf("deploy timeout -> code = %d, want %d", code, CodeDeployTimeout)
	}
	if !bytes.Contains([]byte(out), []byte("滚动更新未完成")) {
		t.Fatalf("expected deploy timeout alert, got: %q", out)
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
	c.alert(feishu.EventDeployTimeout, "app", "deployment not ready")
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
