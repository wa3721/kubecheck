package watcher

import (
	"context"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"

	"kubecheck/internal/checker"
	"kubecheck/internal/options"
)

// ---------- fake 检查器 ----------

// fakeChecker 实现 checkerRunner，隔离真实检查逻辑：
//   - 计数 Run/Interrupt 调用次数（验证触发/去重/中断语义）
//   - 可选 blockRun：Run 阻塞直到 release 关闭（验证 Interrupt 时进行中的检查器仍被遍历）
type fakeChecker struct {
	mu         sync.Mutex
	runCount   int
	interrupts int
	blockRun   bool
	release    chan struct{}
}

func (f *fakeChecker) Run(ctx context.Context) int {
	f.mu.Lock()
	f.runCount++
	f.mu.Unlock()
	if f.blockRun {
		<-f.release
	}
	return checker.CodeOK
}

func (f *fakeChecker) Interrupt() {
	f.mu.Lock()
	f.interrupts++
	f.mu.Unlock()
}

func (f *fakeChecker) getRunCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runCount
}

func (f *fakeChecker) getInterrupts() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.interrupts
}

// ---------- 测试辅助 ----------

// dep 构造一个最小合法的 Deployment
func dep(ns, name string, gen int64) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: ns,
			Generation: gen,
		},
		Spec: appsv1.DeploymentSpec{
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": name}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": name}},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{{Name: "c", Image: "busybox:1.36"}},
				},
			},
		},
	}
}

// newTestWatcher 构造 Watcher（默认全部命名空间），注入 fake 检查器工厂
func newTestWatcher(fc *fakeChecker) (*Watcher, *fake.Clientset) {
	cs := fake.NewSimpleClientset()
	w := New(&options.Options{}, cs, nil)
	w.newChecker = func(_ *options.Options, _ kubernetes.Interface) checkerRunner { return fc }
	return w, cs
}

// newTestWatcherNS 构造带命名空间过滤的 Watcher
func newTestWatcherNS(fc *fakeChecker, ns []string) (*Watcher, *fake.Clientset) {
	cs := fake.NewSimpleClientset()
	w := New(&options.Options{}, cs, ns)
	w.newChecker = func(_ *options.Options, _ kubernetes.Interface) checkerRunner { return fc }
	return w, cs
}

// getHandled 加锁读取 handled map
func (w *Watcher) getHandled(key string) (int64, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	g, ok := w.handled[key]
	return g, ok
}

// getCheckerCount 加锁读取进行中检查器数量
func (w *Watcher) getCheckerCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.checkers)
}

// waitFor 轮询等待条件成立，超时则失败
func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("等待超时（%v）: %s", timeout, msg)
}

// ---------- 测试用例 ----------

// TestNew 验证 New 初始化 maps 与默认工厂
func TestNew(t *testing.T) {
	w := New(&options.Options{}, fake.NewSimpleClientset(), nil)
	if w.handled == nil {
		t.Fatal("handled map 未初始化")
	}
	if w.checkers == nil {
		t.Fatal("checkers map 未初始化")
	}
	if w.newChecker == nil {
		t.Fatal("newChecker 工厂未初始化")
	}
}

// TestNSFilter 验证命名空间过滤器：全量 / 精确 / 多值 / glob 通配 / 空项兜底
func TestNSFilter(t *testing.T) {
	all := newNSFilter(nil)
	if !all.matchAll || !all.match("anything") {
		t.Fatal("nil 过滤应匹配全部")
	}

	f := newNSFilter([]string{"flowtest", " e2e-ns-prod ", "*-prod", "app-?"})
	if !f.match("flowtest") || !f.match("e2e-ns-prod") {
		t.Fatal("精确项应命中")
	}
	if !f.match("my-prod") || !f.match("x-prod") {
		t.Fatal("glob *-prod 应命中 prod 后缀命名空间")
	}
	if !f.match("app-a") || f.match("app-ab") {
		t.Fatal("glob app-? 应仅匹配单字符后缀")
	}
	if f.match("flowtest-dev") || f.match("default") {
		t.Fatal("未匹配项不应命中")
	}

	empty := newNSFilter([]string{"", " "})
	if !empty.matchAll {
		t.Fatal("全空项应兜底为全部")
	}
}

// TestTriggerDedup 验证 trigger 的去重与 generation 递增语义：
//   - 同一 key 相同 generation 不重复触发
//   - 更小 generation 不触发
//   - 更大 generation 触发并更新 handled
//   - 检查完成后从 checkers 中清理
func TestTriggerDedup(t *testing.T) {
	fc := &fakeChecker{}
	w, _ := newTestWatcher(fc)

	// 首次触发 gen=2
	w.triggerUpdate(dep("ns", "app", 2))
	g, ok := w.getHandled("ns/app")
	if !ok || g != 2 {
		t.Fatalf("首次触发后 handled 应为 ns/app=2，实际 ok=%v gen=%d", ok, g)
	}
	waitFor(t, 2*time.Second, "fake 检查器应被调用 1 次", func() bool { return fc.getRunCount() == 1 })

	// 相同 generation 不重复触发
	w.triggerUpdate(dep("ns", "app", 2))
	time.Sleep(50 * time.Millisecond)
	if fc.getRunCount() != 1 {
		t.Fatalf("相同 generation 不应重复触发，当前调用 %d 次", fc.getRunCount())
	}

	// 更小 generation 不触发
	w.triggerUpdate(dep("ns", "app", 1))
	time.Sleep(50 * time.Millisecond)
	if fc.getRunCount() != 1 {
		t.Fatalf("更小 generation 不应触发，当前调用 %d 次", fc.getRunCount())
	}

	// 更大 generation 触发并更新 handled
	w.triggerUpdate(dep("ns", "app", 3))
	waitFor(t, 2*time.Second, "fake 检查器应被调用 2 次", func() bool { return fc.getRunCount() == 2 })
	g, _ = w.getHandled("ns/app")
	if g != 3 {
		t.Fatalf("更大 generation 触发后 handled 应更新为 3，实际 %d", g)
	}

	// 检查完成后 checkers 清理
	waitFor(t, 2*time.Second, "检查完成后 checkers 应清空", func() bool { return w.getCheckerCount() == 0 })
}

// TestTriggerDifferentDeployments 验证不同 Deployment 独立触发、互不干扰
func TestTriggerDifferentDeployments(t *testing.T) {
	fc := &fakeChecker{}
	w, _ := newTestWatcher(fc)

	w.triggerUpdate(dep("ns-a", "app1", 1))
	w.triggerUpdate(dep("ns-b", "app2", 1))
	waitFor(t, 2*time.Second, "两个 Deployment 各触发一次", func() bool { return fc.getRunCount() == 2 })

	if _, ok := w.getHandled("ns-a/app1"); !ok {
		t.Fatal("ns-a/app1 应已记录")
	}
	if _, ok := w.getHandled("ns-b/app2"); !ok {
		t.Fatal("ns-b/app2 应已记录")
	}
	waitFor(t, 2*time.Second, "两个检查器均完成并清理", func() bool { return w.getCheckerCount() == 0 })
}

// TestInterrupt 验证 Interrupt 遍历所有进行中的检查器调用其 Interrupt
func TestInterrupt(t *testing.T) {
	fc := &fakeChecker{blockRun: true, release: make(chan struct{})}
	w, _ := newTestWatcher(fc)

	w.triggerUpdate(dep("ns", "app1", 2))
	w.triggerUpdate(dep("ns", "app2", 2))
	waitFor(t, 2*time.Second, "两个检查器均应登记", func() bool { return w.getCheckerCount() == 2 })

	w.Interrupt()
	if fc.getInterrupts() != 2 {
		t.Fatalf("Interrupt 应调用全部 2 个进行中检查器，实际 %d", fc.getInterrupts())
	}

	// 放行阻塞的 Run，等待清理
	close(fc.release)
	waitFor(t, 2*time.Second, "放行后 checkers 应清空", func() bool { return w.getCheckerCount() == 0 })
}

// TestInterruptNoCheckers 验证无进行中检查器时 Interrupt 安全（不 panic）
func TestInterruptNoCheckers(t *testing.T) {
	w, _ := newTestWatcher(&fakeChecker{})
	w.Interrupt() // 不应 panic
}

// TestRunUpdateEvent 验证 Run 中 informer 的 Update 事件触发语义：
//   - status-only 更新（generation 不变）不触发检查
//   - generation 增加触发检查
//   - ctx 取消后 Run 返回 CodeOK
func TestRunUpdateEvent(t *testing.T) {
	fc := &fakeChecker{}
	w, cs := newTestWatcher(fc)

	// 预置 deployment generation=1（cache sync 前已存在）
	if _, err := cs.AppsV1().Deployments("ns").Create(context.Background(),
		dep("ns", "app", 1), metav1.CreateOptions{}); err != nil {
		t.Fatalf("预置 Deployment 失败: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	go func() { done <- w.Run(ctx) }()

	// 等待 informer 缓存同步完成 + 存量 Add 抑制的 200ms 缓冲
	time.Sleep(500 * time.Millisecond)

	// 存量 Deployment 的初始 Add 不应触发（存量抑制）
	if fc.getRunCount() != 0 {
		t.Fatalf("启动时存量 Add 不应触发检查，实际触发 %d 次", fc.getRunCount())
	}

	// status-only 更新（generation 保持 1）-> 不应触发
	s := dep("ns", "app", 1)
	s.Status.Replicas = 1
	if _, err := cs.AppsV1().Deployments("ns").Update(context.Background(),
		s, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("更新 Deployment 失败: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if fc.getRunCount() != 0 {
		t.Fatalf("status-only 更新不应触发检查，实际触发 %d 次", fc.getRunCount())
	}
	// 存量 Add 已记录 handled=1（存量抑制副产品）；status-only 更新不应改动它
	if g, ok := w.getHandled("ns/app"); !ok || g != 1 {
		t.Fatalf("status-only 更新不应改动 handled，实际 ok=%v gen=%d", ok, g)
	}

	// generation 2 -> 触发检查
	if _, err := cs.AppsV1().Deployments("ns").Update(context.Background(),
		dep("ns", "app", 2), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("更新 Deployment 失败: %v", err)
	}
	waitFor(t, 3*time.Second, "generation 增加应触发检查", func() bool { return fc.getRunCount() == 1 })
	g, _ := w.getHandled("ns/app")
	if g != 2 {
		t.Fatalf("触发后 handled 应为 2，实际 %d", g)
	}

	// 相同 generation 再次更新 -> 不重复触发
	s2 := dep("ns", "app", 2)
	s2.Status.Replicas = 2
	if _, err := cs.AppsV1().Deployments("ns").Update(context.Background(),
		s2, metav1.UpdateOptions{}); err != nil {
		t.Fatalf("更新 Deployment 失败: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if fc.getRunCount() != 1 {
		t.Fatalf("相同 generation 更新不应重复触发，实际触发 %d 次", fc.getRunCount())
	}

	// 取消 ctx -> Run 返回 CodeOK
	cancel()
	select {
	case code := <-done:
		if code != checker.CodeOK {
			t.Fatalf("ctx 取消后 Run 应返回 CodeOK(0)，实际 %d", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run 未在 ctx 取消后返回")
	}
}

// TestRunAddEvent 验证 informer 的 Add 事件触发语义（-A 全命名空间）：
//   - 启动时已存在的存量 Deployment（初始 List 的 Add）不触发（存量抑制）
//   - 运行期新建 Deployment 触发（generation=1 也触发）
//   - 删除后同名重建（generation 重置）仍触发（DeleteFunc 清理 handled）
func TestRunAddEvent(t *testing.T) {
	fc := &fakeChecker{}
	w, cs := newTestWatcher(fc)

	// 预置存量 deployment generation=5（sync 前已存在）
	if _, err := cs.AppsV1().Deployments("ns").Create(context.Background(),
		dep("ns", "existing", 5), metav1.CreateOptions{}); err != nil {
		t.Fatalf("预置 Deployment 失败: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan int, 1)
	go func() { done <- w.Run(ctx) }()

	// 等缓存同步 + 存量抑制缓冲
	time.Sleep(500 * time.Millisecond)
	if fc.getRunCount() != 0 {
		t.Fatalf("存量 Deployment 不应触发检查，实际触发 %d 次", fc.getRunCount())
	}
	if g, ok := w.getHandled("ns/existing"); !ok || g != 5 {
		t.Fatalf("存量 Add 应记录 handled=5 供 Update 去重，实际 ok=%v gen=%d", ok, g)
	}

	// 运行期新建 generation=1 -> 触发（创建语义）
	if _, err := cs.AppsV1().Deployments("ns").Create(context.Background(),
		dep("ns", "fresh", 1), metav1.CreateOptions{}); err != nil {
		t.Fatalf("新建 Deployment 失败: %v", err)
	}
	waitFor(t, 3*time.Second, "运行期新建 Deployment 应触发检查", func() bool { return fc.getRunCount() == 1 })

	// 删除后同名重建（generation 重置为 1）-> 仍触发
	if err := cs.AppsV1().Deployments("ns").Delete(context.Background(),
		"fresh", metav1.DeleteOptions{}); err != nil {
		t.Fatalf("删除 Deployment 失败: %v", err)
	}
	waitFor(t, 3*time.Second, "删除后 handled 应清理", func() bool {
		_, ok := w.getHandled("ns/fresh")
		return !ok
	})
	if _, err := cs.AppsV1().Deployments("ns").Create(context.Background(),
		dep("ns", "fresh", 1), metav1.CreateOptions{}); err != nil {
		t.Fatalf("重建 Deployment 失败: %v", err)
	}
	waitFor(t, 3*time.Second, "删重建（generation 重置）应再次触发", func() bool { return fc.getRunCount() == 2 })

	cancel()
	select {
	case code := <-done:
		if code != checker.CodeOK {
			t.Fatalf("ctx 取消后 Run 应返回 CodeOK(0)，实际 %d", code)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Run 未在 ctx 取消后返回")
	}
}

// TestRunNSFilter 验证 -n 命名空间过滤（glob）：仅匹配命名空间的事件触发
func TestRunNSFilter(t *testing.T) {
	fc := &fakeChecker{}
	w, cs := newTestWatcherNS(fc, []string{"*-prod"})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { w.Run(ctx) }()
	time.Sleep(500 * time.Millisecond) // 等同步

	// 不匹配命名空间：创建 + 更新都不触发
	if _, err := cs.AppsV1().Deployments("flowtest").Create(context.Background(),
		dep("flowtest", "app", 1), metav1.CreateOptions{}); err != nil {
		t.Fatalf("创建失败: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if fc.getRunCount() != 0 {
		t.Fatalf("非匹配命名空间的创建不应触发，实际 %d 次", fc.getRunCount())
	}
	if _, err := cs.AppsV1().Deployments("flowtest").Update(context.Background(),
		dep("flowtest", "app", 2), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("更新失败: %v", err)
	}
	time.Sleep(500 * time.Millisecond)
	if fc.getRunCount() != 0 {
		t.Fatalf("非匹配命名空间的更新不应触发，实际 %d 次", fc.getRunCount())
	}

	// 匹配命名空间（*-prod 后缀）：创建触发
	if _, err := cs.AppsV1().Deployments("e2e-ns-prod").Create(context.Background(),
		dep("e2e-ns-prod", "app", 1), metav1.CreateOptions{}); err != nil {
		t.Fatalf("创建失败: %v", err)
	}
	waitFor(t, 3*time.Second, "匹配命名空间的创建应触发", func() bool { return fc.getRunCount() == 1 })

	// 匹配命名空间：更新（generation 递增）触发
	if _, err := cs.AppsV1().Deployments("e2e-ns-prod").Update(context.Background(),
		dep("e2e-ns-prod", "app", 2), metav1.UpdateOptions{}); err != nil {
		t.Fatalf("更新失败: %v", err)
	}
	waitFor(t, 3*time.Second, "匹配命名空间的更新应触发", func() bool { return fc.getRunCount() == 2 })
}

// TestRunCancelBeforeSync 验证 ctx 提前取消时 Run 走缓存同步失败分支返回 CodeParam
// （WaitForCacheSync 在 stopCh 已关闭时立即返回 false -> CodeParam=1）
func TestRunCancelBeforeSync(t *testing.T) {
	fc := &fakeChecker{}
	w, _ := newTestWatcher(fc)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消
	if code := w.Run(ctx); code != checker.CodeParam {
		t.Fatalf("ctx 已取消时 Run 应返回 CodeParam(1)，实际 %d", code)
	}
}
