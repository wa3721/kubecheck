package watcher

import (
	"context"
	"fmt"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"kubecheck/internal/checker"
	"kubecheck/internal/console"
	"kubecheck/internal/options"
)

// checkerRunner 检查器的运行接口（*checker.Checker 实现）。
// 定义为接口：Watcher 只依赖 Run/Interrupt 两个方法，便于单元测试注入 fake 检查器。
type checkerRunner interface {
	Run(ctx context.Context) int
	Interrupt()
}

// nsFilter 监听模式的命名空间过滤器：
//   - matchAll=true（-A）：匹配任意命名空间
//   - 其余：精确名集合 + 通配模式（含 * 或 ? 的项按 path.Match 匹配，如 *-prod）
type nsFilter struct {
	matchAll bool
	exact    map[string]bool
	patterns []string
}

// newNSFilter 按逗号分隔的过滤串构建过滤器；空列表（nil）表示匹配全部。
// 每项：含 * 或 ? 按通配处理，否则精确匹配；空白项忽略。
func newNSFilter(patternList []string) *nsFilter {
	f := &nsFilter{exact: make(map[string]bool)}
	if len(patternList) == 0 {
		f.matchAll = true
		return f
	}
	for _, p := range patternList {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if strings.ContainsAny(p, "*?") {
			f.patterns = append(f.patterns, p)
		} else {
			f.exact[p] = true
		}
	}
	if len(f.exact) == 0 && len(f.patterns) == 0 {
		f.matchAll = true // 全空项兜底为全部
	}
	return f
}

func (f *nsFilter) match(ns string) bool {
	if f.matchAll {
		return true
	}
	if f.exact[ns] {
		return true
	}
	for _, p := range f.patterns {
		if ok, err := path.Match(p, ns); err == nil && ok {
			return true
		}
	}
	return false
}

// describe 供启动日志展示监听范围
func (f *nsFilter) describe() string {
	if f.matchAll {
		return "全部命名空间"
	}
	var parts []string
	if len(f.exact) > 0 {
		for k := range f.exact {
			parts = append(parts, k)
		}
	}
	parts = append(parts, f.patterns...)
	return "命名空间过滤 [" + strings.Join(parts, ", ") + "]"
}

// Watcher 常驻监听模式：监听命名空间（全部或按 -n 过滤）的 Deployment。
// Deployment 创建或 spec 变更（generation 递增）时，自动对该 Deployment
// 启动现有检查逻辑（复用 checker.New + checker.Run，不改动任何现有逻辑）。
// 纯 status 变化（副本数上报、ready 更新等）不触发检查——只有创建与模板/spec
// 变化（含 kubectl rollout restart，其通过修改 pod-template annotation 递增
// generation）才触发，与"发生创建/更新则自动检查"的需求一致。
//
// 存量抑制：informer 初始 List 会对所有已存在的 Deployment 触发 Add 事件，
// 这些"存量 Add"不触发检查（否则 -A 启动瞬间会对集群全部存量 Deployment 各跑
// 一次检查）；仅缓存同步完成（synced）之后的新建 Add（含删除后同名重建）触发。
type Watcher struct {
	opts      *options.Options
	clientset kubernetes.Interface
	filter    *nsFilter
	synced    atomic.Bool // informer 初始 List 完成标志（存量 Add 抑制开关）

	// newChecker 创建检查器实例的工厂，默认基于 checker.New 构造真实检查器；
	// 单元测试中可注入 fake，隔离检查逻辑、专注测试 Watcher 自身的触发/去重/中断。
	newChecker func(opts *options.Options, cs kubernetes.Interface) checkerRunner

	mu       sync.Mutex
	handled  map[string]int64          // ns/name -> 已触发过的 generation（防重复触发）
	checkers map[string]checkerRunner // ns/name -> 进行中的检查实例
}

// New 构造 Watcher。namespaces 为命名空间过滤（nil/空 = 全部，-A）。
func New(opts *options.Options, cs kubernetes.Interface, namespaces []string) *Watcher {
	return &Watcher{
		opts:       opts,
		clientset:  cs,
		filter:     newNSFilter(namespaces),
		handled:    make(map[string]int64),
		checkers:   make(map[string]checkerRunner),
		newChecker: func(o *options.Options, k kubernetes.Interface) checkerRunner {
			return checker.New(o, k)
		},
	}
}

// Run 常驻监听（命名空间范围由构造参数决定），直到 ctx 被取消。
// 返回退出码（0=正常结束，其余为启动失败）。
func (w *Watcher) Run(ctx context.Context) int {
	// 单一全命名空间 informer + 事件侧 ns 过滤：精确/多值/glob 统一处理
	// （避免为每个 ns 建 factory，glob 也无需预知 ns 列表）
	factory := informers.NewSharedInformerFactory(w.clientset, 0)
	informer := factory.Apps().V1().Deployments().Informer()
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			d, ok := obj.(*appsv1.Deployment)
			if !ok {
				return
			}
			if !w.synced.Load() {
				// 初始 List 的存量 Add：仅记录 generation（供后续 Update 去重），不触发
				w.mu.Lock()
				w.handled[d.Namespace+"/"+d.Name] = d.Generation
				w.mu.Unlock()
				return
			}
			// 运行期新建（含删除后同名重建）：无条件触发（重建的 generation 会从 1 重置，
			// 不能沿用 handled 的 "更大才触发" 判定）
			w.triggerCreate(d)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldD, ok1 := oldObj.(*appsv1.Deployment)
			newD, ok2 := newObj.(*appsv1.Deployment)
			if !ok1 || !ok2 {
				return
			}
			// 仅 generation 增加视为一次"更新"；Deployment 的 generation
			// 只在 spec 变化时递增，status 上报不会改变 generation
			if newD.Generation > oldD.Generation {
				w.triggerUpdate(newD)
			}
		},
		DeleteFunc: func(obj interface{}) {
			// 删除后清理 handled：同名重建（generation 重置为 1）时 Add 才能正常触发
			d, ok := obj.(*appsv1.Deployment)
			if !ok {
				return
			}
			w.mu.Lock()
			delete(w.handled, d.Namespace+"/"+d.Name)
			w.mu.Unlock()
		},
	}); err != nil {
		fmt.Println(console.Red(fmt.Sprintf("[ERROR] 注册 Deployment 监听失败: %v", err)))
		return checker.CodeParam
	}

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		fmt.Println(console.Red("[ERROR] Deployment 缓存同步失败"))
		return checker.CodeParam
	}
	// 缓冲：WaitForCacheSync 返回时初始 List 的存量 Add 事件可能尚有极少量在分发，
	// 短暂等待后再放行"新建触发"，消除"启动瞬间对存量误触发"的竞态窗口
	time.Sleep(200 * time.Millisecond)
	w.synced.Store(true)
	fmt.Println(console.Green(fmt.Sprintf("[INFO] 监听模式已启动：%s 的 Deployment 创建或更新（generation 递增）时自动执行检查（存量不触发）",
		w.filter.describe())))

	<-ctx.Done()
	return checker.CodeOK
}

// startCheck 公共触发入口：ns 过滤 + generation 去重 + 启动检查 goroutine。
// reason 用于启动日志（创建/更新）。
func (w *Watcher) startCheck(d *appsv1.Deployment, reason string) {
	if !w.filter.match(d.Namespace) {
		return
	}
	key := d.Namespace + "/" + d.Name

	// 同一 Deployment 仅对最新 generation 触发一次检查（天然覆盖滚动期间的多次事件）
	w.mu.Lock()
	if gen, ok := w.handled[key]; ok && d.Generation <= gen {
		w.mu.Unlock()
		return
	}
	w.handled[key] = d.Generation
	w.mu.Unlock()

	// 值拷贝 opts：为每个 Deployment 单独构造检查器，避免并发共享同一 opts
	checkOpts := *w.opts
	checkOpts.Namespace = d.Namespace
	checkOpts.ResourceName = d.Name
	chk := w.newChecker(&checkOpts, w.clientset)

	w.mu.Lock()
	w.checkers[key] = chk
	w.mu.Unlock()

	fmt.Println(console.Green(fmt.Sprintf("[INFO] 检测到 Deployment %s/%s %s（generation=%d），启动检查",
		d.Namespace, d.Name, reason, d.Generation)))
	go func() {
		code := chk.Run(context.Background())
		fmt.Println(console.Green(fmt.Sprintf("[INFO] Deployment %s/%s 检查完成，退出码=%d", d.Namespace, d.Name, code)))
		w.mu.Lock()
		if cur, ok := w.checkers[key]; ok && cur == chk {
			delete(w.checkers, key)
		}
		w.mu.Unlock()
	}()
}

// triggerCreate 运行期新建（含同名删重建）：无条件走 generation 去重外的
// 正常触发（handled 已在 DeleteFunc/初次 Add 中维护）。
func (w *Watcher) triggerCreate(d *appsv1.Deployment) {
	w.startCheck(d, "创建")
}

// triggerUpdate spec 变更（generation 递增）
func (w *Watcher) triggerUpdate(d *appsv1.Deployment) {
	w.startCheck(d, "更新")
}

// Interrupt 中断所有进行中的检查（供 SIGINT/SIGTERM 时统一调用）。
// 每个 Checker.Interrupt 会取消其全部超时上下文并等待日志落盘完成。
func (w *Watcher) Interrupt() {
	w.mu.Lock()
	list := make([]checkerRunner, 0, len(w.checkers))
	for _, chk := range w.checkers {
		list = append(list, chk)
	}
	w.mu.Unlock()
	for _, chk := range list {
		chk.Interrupt()
	}
}
