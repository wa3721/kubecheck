package watcher

import (
	"context"
	"fmt"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"kubecheck/internal/checker"
	"kubecheck/internal/options"
)

// checkerRunner 检查器的运行接口（*checker.Checker 实现）。
// 定义为接口：Watcher 只依赖 Run/Interrupt 两个方法，便于单元测试注入 fake 检查器。
type checkerRunner interface {
	Run(ctx context.Context) int
	Interrupt()
}

// Watcher -A 常驻监听模式：监听所有命名空间的 Deployment。
// 检测到 Deployment spec 变更（generation 增加）时，自动对该 Deployment
// 启动现有检查逻辑（复用 checker.New + checker.Run，不改动任何现有逻辑）。
// 纯 status 变化（副本数上报、ready 更新等）不触发检查——只有模板/spec 变化
// （含 kubectl rollout restart，其通过修改 pod-template annotation 递增 generation）
// 才会触发，与"发生更新则自动检查"的需求一致。
type Watcher struct {
	opts      *options.Options
	clientset kubernetes.Interface

	// newChecker 创建检查器实例的工厂，默认基于 checker.New 构造真实检查器；
	// 单元测试中可注入 fake，隔离检查逻辑、专注测试 Watcher 自身的触发/去重/中断。
	newChecker func(opts *options.Options, cs kubernetes.Interface) checkerRunner

	mu       sync.Mutex
	handled  map[string]int64          // ns/name -> 已触发过的 generation（防重复触发）
	checkers map[string]checkerRunner // ns/name -> 进行中的检查实例
}

// New 构造 Watcher
func New(opts *options.Options, cs kubernetes.Interface) *Watcher {
	return &Watcher{
		opts:      opts,
		clientset: cs,
		handled:   make(map[string]int64),
		checkers:  make(map[string]checkerRunner),
		newChecker: func(o *options.Options, k kubernetes.Interface) checkerRunner {
			return checker.New(o, k)
		},
	}
}

// Run 常驻监听所有命名空间的 Deployment，直到 ctx 被取消。
// 返回退出码（0=正常结束，其余为启动失败）。
func (w *Watcher) Run(ctx context.Context) int {
	factory := informers.NewSharedInformerFactory(w.clientset, 0)
	informer := factory.Apps().V1().Deployments().Informer()
	if _, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldD, ok1 := oldObj.(*appsv1.Deployment)
			newD, ok2 := newObj.(*appsv1.Deployment)
			if !ok1 || !ok2 {
				return
			}
			// 仅 generation 增加视为一次"更新"；Deployment 的 generation
			// 只在 spec 变化时递增，status 上报不会改变 generation
			if newD.Generation > oldD.Generation {
				w.trigger(newD)
			}
		},
	}); err != nil {
		fmt.Printf("[ERROR] 注册 Deployment 更新监听失败: %v\n", err)
		return checker.CodeParam
	}

	factory.Start(ctx.Done())
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		fmt.Printf("[ERROR] Deployment 缓存同步失败\n")
		return checker.CodeParam
	}
	fmt.Printf("[INFO] -A 模式已启动：监听所有命名空间的 Deployment 更新，发生更新时自动执行检查\n")

	<-ctx.Done()
	return checker.CodeOK
}

// trigger 对发生更新的 Deployment 启动一次独立检查。
// 每个 Deployment 并发独立执行（各自的超时上下文/告警去重/日志落盘），互不影响。
func (w *Watcher) trigger(d *appsv1.Deployment) {
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

	fmt.Printf("[INFO] 检测到 Deployment %s/%s 更新（generation=%d），启动检查\n",
		d.Namespace, d.Name, d.Generation)
	go func() {
		code := chk.Run(context.Background())
		fmt.Printf("[INFO] Deployment %s/%s 检查完成，退出码=%d\n", d.Namespace, d.Name, code)
		w.mu.Lock()
		if cur, ok := w.checkers[key]; ok && cur == chk {
			delete(w.checkers, key)
		}
		w.mu.Unlock()
	}()
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
