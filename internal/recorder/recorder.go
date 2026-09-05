package recorder

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"kubecheck/internal/console"
)

// 错误行红色标注（ANSI 转义码）
const (
	redStart = "\x1b[31m"
	redEnd   = "\x1b[0m"
)

// LogErrorRecorder 错误日志收集层：以 Follow 模式持续跟踪 Pod 日志，
// 命中错误关键字时才将"启动到报错"的日志落盘至 {namespace}-{podName}-{date}.log，
// 错误行红色标注；未命中错误且容器未报错退出的应用不产生日志文件。
//
// dump=false（--log-dump 默认关闭）时不落盘：日志检测/仲裁/errorHit 判定照常执行，
// 仅跳过全部文件写入与路径登记（RecordedPath 恒为空，告警不附带落盘路径）。
//
// console=true（--log-console 默认开启）时把追踪流读到的日志行逐行实时输出到
// 控制台（原始行，效果同 kubectl logs --follow）；与关键字判定/LLM 仲裁/落盘互不影响。
type LogErrorRecorder struct {
	dir     string
	dump    bool
	console bool

	mu       sync.Mutex
	recorded map[string]string // podKey(ns/name) -> 已落盘文件路径
}

func New(dir string, dump, console bool) *LogErrorRecorder {
	return &LogErrorRecorder{
		dir:      dir,
		dump:     dump,
		console:  console,
		recorded: make(map[string]string),
	}
}

func podKey(ns, name string) string { return ns + "/" + name }

// FilePath 生成落盘文件路径: {dir}/{ns}-{podName}-{date}.log（date 格式 20060102，跨天自动切换）
func (r *LogErrorRecorder) FilePath(ns, podName string) string {
	date := time.Now().Format("20060102")
	return filepath.Join(r.dir, fmt.Sprintf("%s-%s-%s.log", ns, podName, date))
}

// RecordedPath 返回某 Pod 已落盘的错误日志文件路径（无记录返回空串）
func (r *LogErrorRecorder) RecordedPath(ns, podName string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.recorded[podKey(ns, podName)]
}

// ErrorJudge 日志错误仲裁器（依赖倒置，便于单测注入 fake）。
// *llm.Client 实现该接口；nil 或 Enabled() == false 时 TrackLog 走
// "关键字即真"的现行逻辑（行为与未引入仲裁前一致）。
type ErrorJudge interface {
	// Enabled endpoint 非空即启用
	Enabled() bool
	// Timeout 单次判定的超时时长（供调用方计算在途判定的等待上限）
	Timeout() time.Duration
	// Judge 批量判定日志行是否为真实错误，返回与 lines 等长的布尔切片；
	// 调用失败返回 error，由调用方降级为"关键字即真"
	Judge(ctx context.Context, lines []string) ([]bool, error)
}

// TrackLog 在 ctx 生命周期内实时跟踪 Pod 日志，按是否启用 LLM 仲裁分两条路径：
//
//   - 未启用（judge 为 nil 或 Enabled() == false）：现行逻辑——命中错误关键字即视为真错误，
//     首次命中把"从日志起点（含 --log-tail 回溯）到报错行"的缓冲日志一次性落盘，之后增量追加
//   - 启用：命中错误关键字的行投入 pod 级仲裁 channel，LLM 异步批量判定真伪；
//     存在任一真错误 -> 以容器启动时刻为起点全量拉取日志覆盖写快照；全部假错误 -> 不落盘；
//     判定调用失败 -> 降级为真错误（宁可误报不漏报）
//
// 两条路径共同点：容器异常退出（退出码 != 0）不经仲裁，无条件落盘保留现场；
// 正常应用（未命中错误且容器未报错退出）不产生日志文件。
// --log-dump 关闭（New 的 dump=false）时全部落盘动作跳过，检测/errorHit 判定不受影响。
// 返回 (lines 累计读取行数, errorHit 是否存在真错误, err)。
//
// 流式策略（兼顾 CrashLoopBackOff 等快速退出场景，两路径共用）：
//   - 容器 Running：优先 Follow 持续跟踪；开流失败（容器快速退出竞态）时
//     降级为非 Follow 一次性拉取历史日志
//   - 容器已 Terminated / 等待重启（有 containerID，即曾启动过）：非 Follow 拉取
//   - 容器从未启动（ContainerCreating，无 containerID）：跳过
func (r *LogErrorRecorder) TrackLog(ctx context.Context, clientset kubernetes.Interface,
	pod *corev1.Pod, judge ErrorJudge, errKw, ignoreKw []string, tail int) (int, bool, error) {

	if judge != nil && judge.Enabled() {
		return r.trackLogWithJudge(ctx, clientset, pod, judge, errKw, ignoreKw, tail)
	}
	return r.trackLogKeyword(ctx, clientset, pod, errKw, ignoreKw, tail)
}

// trackLogKeyword 未启用 LLM 仲裁的现行路径（行为与历史版本一致）：
// 命中错误关键字时才落盘——首次命中时把"从日志起点（含 --log-tail 回溯）到
// 报错行"的缓冲日志一次性写入文件，此后多次命中错误则逐行增量追加更新。
func (r *LogErrorRecorder) trackLogKeyword(ctx context.Context, clientset kubernetes.Interface,
	pod *corev1.Pod, errKw, ignoreKw []string, tail int) (int, bool, error) {
	var (
		errorHit bool
		count    int
	)

	// 等待容器进入 Running：Running 时 Follow 流才可靠；已退出容器由下方非 Follow 兜底
	pod = r.waitForContainerRunning(ctx, clientset, pod)

	path := r.FilePath(pod.Namespace, pod.Name)
	var f *os.File // 首次命中错误/容器报错退出时才打开；nil 表示尚未落盘

	// 命中错误前先缓冲日志（环形缓冲，容量随 --log-tail 放大，避免内存无界增长）；
	// 首次命中错误时把缓冲的"启动到报错"日志一次性落盘，之后增量追加。
	buf := newLineRing(max(tail*10, 1000))
	// flushBuf 把缓冲日志全部写入文件；首次调用时必须打开文件（即使缓冲为空，
	// 后续错误行仍需写入，避免对 nil 文件句柄 Fprintf 报 invalid argument）
	flushBuf := func() error {
		if f == nil {
			nf, err := appendFile(path)
			if err != nil {
				return fmt.Errorf("打开日志文件 %s 失败: %w", path, err)
			}
			f = nf
		}
		for _, l := range buf.drain() {
			if _, werr := fmt.Fprintf(f, "%s\n", l); werr != nil {
				return werr
			}
		}
		return nil
	}

	for _, cs := range pod.Status.ContainerStatuses {
		// 从未启动过的容器（ContainerCreating，无 containerID）跳过，无日志可拉
		if cs.State.Running == nil && cs.ContainerID == "" {
			continue
		}
		// Running 容器 Follow 跟踪；已退出/等待重启容器非 Follow 一次性拉取。
		// 注意：容器退出时 logCtx 已被 cancelLog 取消，Follow 流会立刻断开，
		// 故非 Follow 兜底统一用独立短超时 ctx，确保取消后仍能拉到历史日志。
		follow := cs.State.Running != nil && ctx.Err() == nil
		opts := &corev1.PodLogOptions{
			Container:  cs.Name,
			Follow:     follow,
			Timestamps: true,
		}
		if tail > 0 {
			opts.TailLines = int64Ptr(int64(tail))
		}
		var (
			stream io.ReadCloser
			err    error
		)
		if follow {
			stream, err = clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, opts).Stream(ctx)
		}
		if err != nil || !follow {
			// Follow 开流失败（容器在建立连接期间退出）或容器已非 Running：
			// 降级为非 Follow，用独立短超时 ctx 拉历史日志（不被 logCtx 取消影响）
			opts.Follow = false
			pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
			stream, err = clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, opts).Stream(pctx)
			if err == nil {
				defer pcancel()
			} else {
				pcancel()
			}
			follow = false
		}
		if err != nil {
			if ctx.Err() == nil {
				fmt.Println(console.Yellow(fmt.Sprintf("[WARN] 跟踪 %s/%s 容器 %s 日志流失败: %v", pod.Namespace, pod.Name, cs.Name, err)))
			}
			continue
		}
		sc := bufio.NewScanner(stream)
		for sc.Scan() {
			line := sc.Text()
			count++
			if r.console {
				fmt.Println(line) // --log-console：追踪流逐行实时输出（同 kubectl logs -f）
			}
			if hitAny(line, errKw) && !hitIgnore(line, ignoreKw) {
				errorHit = true
				// 首次命中错误：缓冲日志 + 本行一次性落盘；之后增量追加
				//（--log-dump 关闭时跳过写文件，仅保留 errorHit 判定）
				if r.dump {
					if err := flushBuf(); err != nil {
						_ = stream.Close()
						return count, errorHit, err
					}
					if _, werr := fmt.Fprintf(f, "%s\n", colorizeErrorLine(line, cs.Name, errKw, ignoreKw)); werr != nil {
						_ = stream.Close()
						return count, errorHit, werr
					}
				}
				continue
			}
			if f == nil {
				// 未落盘前缓冲；已落盘后继续增量追加
				buf.add(colorizeErrorLine(line, cs.Name, errKw, ignoreKw))
			} else if _, werr := fmt.Fprintf(f, "%s\n", colorizeErrorLine(line, cs.Name, errKw, ignoreKw)); werr != nil {
				_ = stream.Close()
				return count, errorHit, werr
			}
		}
		_ = stream.Close()
		if ctx.Err() != nil && !follow {
			// 非 Follow 已读到容器历史日志末尾，结束遍历
			break
		}
	}

	// 循环结束收尾：不依赖 ctx 状态，直接查 Pod 判断容器是否已异常退出——
	// 未命中错误但容器异常退出（exit code != 0）-> 把缓冲日志落盘保留现场；
	// 其余情况（正常超时/正常退出/未退出）不落盘。
	if r.dump && f == nil && buf.len() > 0 {
		if p, gerr := clientset.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{}); gerr == nil {
			if _, exited := containerExited(p); exited {
				if werr := flushBuf(); werr != nil {
					return count, errorHit, werr
				}
			}
		}
	}

	if f != nil {
		if cerr := f.Close(); cerr != nil {
			return count, errorHit, cerr
		}
		r.mu.Lock()
		r.recorded[podKey(pod.Namespace, pod.Name)] = path
		r.mu.Unlock()
	}
	return count, errorHit, nil
}

// 仲裁参数：channel 容量 / 攒批行数 / 攒批时间窗 / 收尾 flush 的最大行数 / 熔断阈值
const (
	arbChannelCap    = 256
	arbBatchLines    = 10
	arbBatchWindow   = 2 * time.Second
	arbFinalMaxLines = 40
	arbMaxFails      = 3
)

// arbBreaker 仲裁熔断器：LLM 端点持续不可达（连续 arbMaxFails 次调用失败）后熔断，
// 后续命中行直接按"关键字即真"处理，不再发起无意义的网络调用（避免 WARN 刷屏与重试开销）。
// 非并发安全：仅仲裁 goroutine 内使用。
type arbBreaker struct {
	threshold int
	fails     int
	tripped   bool
}

func newArbBreaker() *arbBreaker { return &arbBreaker{threshold: arbMaxFails} }

// allow 是否仍允许发起判定调用
func (b *arbBreaker) allow() bool { return !b.tripped }

// recordSuccess 调用成功，重置失败计数
func (b *arbBreaker) recordSuccess() { b.fails = 0 }

// recordFail 记录一次失败；达到阈值时熔断并返回 true（仅首次返回 true，用于打印熔断告警）
func (b *arbBreaker) recordFail() bool {
	b.fails++
	if !b.tripped && b.fails >= b.threshold {
		b.tripped = true
		return true
	}
	return false
}

// allTrue 生成全 true 的判定结果（降级/熔断时的"关键字即真"）
func allTrue(n int) []bool {
	res := make([]bool, n)
	for i := range res {
		res[i] = true
	}
	return res
}

// trackLogWithJudge LLM 仲裁路径：
// 命中错误关键字的行投入 pod 级有界 channel（满则丢弃并计数，不阻塞日志流），
// 仲裁 goroutine 攒批（10 行 / 2 秒）+ 相同行去重后批量判定：
//   - 任一真错误（且该行此前未判定为真）-> 全量快照落盘 + errorHit=true
//   - 全部假错误 -> 不落盘
//   - 判定调用失败 -> 降级为真错误（宁可误报不漏报）
//
// 全部容器日志流结束后 close(channel)，仲裁 goroutine 收尾 flush（上限 40 行，
// 超出丢弃并告警），随后 TrackLog 等待其在途判定完成（上限 2×timeout + 10s）。
// 容器异常退出（退出码 != 0）且未经仲裁命中真错误时，无条件全量快照保留现场
// （不经 LLM，与现行路径语义一致：落盘但不算 errorHit）。
func (r *LogErrorRecorder) trackLogWithJudge(ctx context.Context, clientset kubernetes.Interface,
	pod *corev1.Pod, judge ErrorJudge, errKw, ignoreKw []string, tail int) (int, bool, error) {

	var (
		count    int
		errorHit bool
		dropped  int
	)
	// 保护 errorHit（仲裁 goroutine 写、本函数读）
	var mu sync.Mutex
	// 快照互斥：真错误快照与退出兜底快照不并发写同一文件
	var snapMu sync.Mutex

	takeSnapshot := func(reason string) {
		if !r.dump {
			return // --log-dump 关闭：不落盘，仅保留 errorHit/告警语义
		}
		snapMu.Lock()
		defer snapMu.Unlock()
		sctx, scancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer scancel()
		if err := r.snapshotFull(sctx, clientset, pod, errKw, ignoreKw); err != nil {
			fmt.Println(console.Yellow(fmt.Sprintf("[WARN] Pod %s/%s 全量日志快照落盘失败（%s）: %v", pod.Namespace, pod.Name, reason, err)))
			return
		}
	}

	pod = r.waitForContainerRunning(ctx, clientset, pod)

	hitCh := make(chan string, arbChannelCap)
	arbDone := make(chan struct{})

	// 仲裁 goroutine：消费命中行，攒批去重后调用 LLM 批量判定
	go func() {
		defer close(arbDone)
		judged := make(map[string]bool) // 行内容 -> 判定结果（去重缓存：相同行不重复判定）
		var pending []string
		breaker := newArbBreaker()

		judgePending := func() {
			if len(pending) == 0 {
				return
			}
			lines := pending
			pending = nil
			// 判定使用独立超时上下文：窗口结束/取消后仍能完成在途判定（有界）
			var res []bool
			if breaker.allow() {
				var err error
				// 未熔断：正常调用 LLM 批量判定
				res, err = judge.Judge(context.Background(), lines)
				if err != nil {
					// 调用失败：降级为真错误（宁可误报不漏报）；持续失败则熔断
					if breaker.recordFail() {
						fmt.Println(console.Yellow(fmt.Sprintf("[WARN] LLM 日志仲裁连续 %d 次调用失败，熔断：后续命中按关键字即真处理，不再重试", breaker.threshold)))
					} else {
						fmt.Println(console.Yellow(fmt.Sprintf("[WARN] LLM 日志仲裁调用失败，降级为关键字即真: %v", err)))
					}
					res = allTrue(len(lines))
				} else {
					breaker.recordSuccess()
				}
			} else {
				// 已熔断：不再发起网络调用，直接按关键字即真
				res = allTrue(len(lines))
			}
			hasNewTrue := false
			for i, line := range lines {
				if res[i] && !judged[line] {
					hasNewTrue = true
				}
				judged[line] = res[i]
			}
			if hasNewTrue {
				// 存在新的真错误 -> 全量快照（覆盖写更新）
				takeSnapshot("LLM 判定真错误")
				mu.Lock()
				errorHit = true
				mu.Unlock()
			}
		}

		ticker := time.NewTicker(arbBatchWindow)
		defer ticker.Stop()
		for {
			select {
			case line, ok := <-hitCh:
				if !ok {
					// 收尾 flush：超限行丢弃（有界，避免长时间阻塞退出）
					if len(pending) > arbFinalMaxLines {
						fmt.Println(console.Yellow(fmt.Sprintf("[WARN] 收尾仲裁行数 %d 超上限 %d，仅判定前 %d 行",
							len(pending), arbFinalMaxLines, arbFinalMaxLines)))
						pending = pending[:arbFinalMaxLines]
					}
					judgePending()
					return
				}
				if _, seen := judged[line]; seen {
					continue // 去重：相同行复用既有判定（含在途占位）
				}
				judged[line] = false // 占位：判定完成前相同行不重复入队
				pending = append(pending, line)
				if len(pending) >= arbBatchLines {
					judgePending()
				}
			case <-ticker.C:
				judgePending()
			}
		}
	}()

	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Running == nil && cs.ContainerID == "" {
			continue
		}
		follow := cs.State.Running != nil && ctx.Err() == nil
		opts := &corev1.PodLogOptions{
			Container:  cs.Name,
			Follow:     follow,
			Timestamps: true,
		}
		if tail > 0 {
			opts.TailLines = int64Ptr(int64(tail))
		}
		var (
			stream io.ReadCloser
			err    error
		)
		if follow {
			stream, err = clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, opts).Stream(ctx)
		}
		if err != nil || !follow {
			opts.Follow = false
			pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
			stream, err = clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, opts).Stream(pctx)
			if err == nil {
				defer pcancel()
			} else {
				pcancel()
			}
			follow = false
		}
		if err != nil {
			if ctx.Err() == nil {
				fmt.Println(console.Yellow(fmt.Sprintf("[WARN] 跟踪 %s/%s 容器 %s 日志流失败: %v", pod.Namespace, pod.Name, cs.Name, err)))
			}
			continue
		}
		sc := bufio.NewScanner(stream)
		for sc.Scan() {
			line := sc.Text()
			count++
			if r.console {
				fmt.Println(line) // --log-console：追踪流逐行实时输出（同 kubectl logs -f）
			}
			if hitAny(line, errKw) && !hitIgnore(line, ignoreKw) {
				select {
				case hitCh <- line:
				default:
					dropped++ // 有界 channel 满：丢弃并计数，绝不阻塞日志流
				}
			}
			// 非命中行无需处理：真错误时全量快照会重新拉取完整日志
		}
		_ = stream.Close()
		if ctx.Err() != nil && !follow {
			break
		}
	}
	close(hitCh)

	// 等待仲裁 goroutine 完成（上限：2×单次判定超时 + 快照余量）
	arbWait := 2*judge.Timeout() + 10*time.Second
	select {
	case <-arbDone:
	case <-time.After(arbWait):
		fmt.Println(console.Yellow(fmt.Sprintf("[WARN] Pod %s/%s 仲裁收尾超时（%v），不再等待", pod.Namespace, pod.Name, arbWait)))
	}

	// 容器异常退出兜底：未经仲裁命中真错误时无条件全量快照保留现场（不计 errorHit）
	mu.Lock()
	hit := errorHit
	mu.Unlock()
	if !hit {
		if p, gerr := clientset.CoreV1().Pods(pod.Namespace).Get(context.Background(), pod.Name, metav1.GetOptions{}); gerr == nil {
			if _, exited := containerExited(p); exited {
				takeSnapshot("容器异常退出")
			}
		}
	}
	if dropped > 0 {
		fmt.Println(console.Yellow(fmt.Sprintf("[WARN] Pod %s/%s 有 %d 行命中日志因仲裁队列满被丢弃", pod.Namespace, pod.Name, dropped)))
	}
	return count, hit, nil
}

// snapshotFull 全量快照：以容器启动时刻为起点（SinceTime）逐容器拉取日志，
// 覆盖写 {dir}/{ns}-{pod}-{date}.log——首容器 O_TRUNC（覆盖旧快照），后续容器追加；
// 错误行红色标注。容器启动时间取 State.Running.StartedAt（已退出取
// LastTerminationState.Terminated.StartedAt，均不可得回退 Pod 创建时间）。
func (r *LogErrorRecorder) snapshotFull(ctx context.Context, clientset kubernetes.Interface,
	pod *corev1.Pod, errKw, ignoreKw []string) error {

	if !r.dump {
		return nil // 防御性短路：正常情况下 takeSnapshot 已拦截
	}

	// 读取最新状态（容器状态/启动时间可能已变化）
	if p, err := clientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{}); err == nil {
		pod = p
	}

	since := containerStartTime(pod)
	path := r.FilePath(pod.Namespace, pod.Name)

	wrote := false
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Running == nil && cs.ContainerID == "" {
			continue // 从未启动过的容器无日志
		}
		opts := &corev1.PodLogOptions{
			Container:  cs.Name,
			Timestamps: true,
			SinceTime:  &since,
		}
		stream, err := clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, opts).Stream(ctx)
		if err != nil {
			continue // 单容器拉取失败不阻断其余容器
		}
		f, err := truncFile(path, !wrote) // 首个可拉取容器 O_TRUNC，其余追加
		if err != nil {
			_ = stream.Close()
			return err
		}
		sc := bufio.NewScanner(stream)
		for sc.Scan() {
			if _, werr := fmt.Fprintf(f, "%s\n", colorizeErrorLine(sc.Text(), cs.Name, errKw, ignoreKw)); werr != nil {
				_ = stream.Close()
				_ = f.Close()
				return werr
			}
		}
		_ = stream.Close()
		if cerr := f.Close(); cerr != nil {
			return cerr
		}
		wrote = true
	}
	if wrote {
		r.mu.Lock()
		r.recorded[podKey(pod.Namespace, pod.Name)] = path
		r.mu.Unlock()
	}
	return nil
}

// containerStartTime 取 Pod 全量日志拉取起点（SinceTime）：
// 优先当前 Running 容器的启动时间，其次上次退出容器的启动时间，
// 均不可得回退 Pod 创建时间。多容器取最早，确保覆盖全部容器日志。
func containerStartTime(p *corev1.Pod) metav1.Time {
	var earliest *metav1.Time
	for _, cs := range p.Status.ContainerStatuses {
		var t *metav1.Time
		switch {
		case cs.State.Running != nil:
			t = &cs.State.Running.StartedAt
		case cs.LastTerminationState.Terminated != nil:
			t = &cs.LastTerminationState.Terminated.StartedAt
		}
		if t != nil && (earliest == nil || t.Before(earliest)) {
			earliest = t
		}
	}
	if earliest != nil {
		return *earliest
	}
	return p.CreationTimestamp
}

// truncFile 打开落盘文件：trunc=true 时 O_TRUNC（覆盖旧快照），否则 O_APPEND（多容器追加）。
func truncFile(path string, trunc bool) (*os.File, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	flag := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	if trunc {
		flag = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	}
	return os.OpenFile(path, flag, 0o644)
}

// containerExited 判断任一容器是否 Terminated 且退出码非 0，返回 (退出码, true)
func containerExited(p *corev1.Pod) (int32, bool) {
	if p == nil {
		return 0, false
	}
	for _, cs := range p.Status.ContainerStatuses {
		if cs.State.Terminated != nil && cs.State.Terminated.ExitCode != 0 {
			return cs.State.Terminated.ExitCode, true
		}
	}
	return 0, false
}

// lineRing 有界环形缓冲：容量满时丢弃最旧行（FIFO），保证内存有界。
type lineRing struct {
	items []string
	head  int
	size  int
	cap   int
}

func newLineRing(cap int) *lineRing {
	if cap < 1 {
		cap = 1
	}
	return &lineRing{items: make([]string, cap), cap: cap}
}

func (b *lineRing) add(line string) {
	if b.size < b.cap {
		b.items[(b.head+b.size)%b.cap] = line
		b.size++
		return
	}
	b.items[b.head] = line
	b.head = (b.head + 1) % b.cap
}

// drain 按时间顺序取出全部缓冲行并清空缓冲
func (b *lineRing) drain() []string {
	out := make([]string, 0, b.size)
	for i := 0; i < b.size; i++ {
		out = append(out, b.items[(b.head+i)%b.cap])
	}
	b.head = 0
	b.size = 0
	return out
}

func (b *lineRing) len() int { return b.size }

// waitForContainerRunning 在 ctx 内轮询 Pod，直到至少一个容器进入 Running 或 ctx 取消。
// 返回最新的 Pod 对象（容器状态已更新）。
func (r *LogErrorRecorder) waitForContainerRunning(ctx context.Context, clientset kubernetes.Interface, pod *corev1.Pod) *corev1.Pod {
	// 立即满足则直接返回
	if anyContainerRunning(pod) {
		return pod
	}
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return pod
		case <-ticker.C:
			p, err := clientset.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
			if err != nil {
				return pod
			}
			pod = p
			if anyContainerRunning(pod) {
				return pod
			}
		}
	}
}

// anyContainerRunning 判断 Pod 是否至少有一个容器处于 Running 状态
func anyContainerRunning(pod *corev1.Pod) bool {
	for _, cs := range pod.Status.ContainerStatuses {
		if cs.State.Running != nil {
			return true
		}
	}
	return false
}

func int64Ptr(v int64) *int64 { return &v }

// colorizeErrorLine 命中错误关键字的行加红色标注，忽略关键字的行不标红；格式 [时间戳] [container=xx] 原文
func colorizeErrorLine(line, container string, errKw, ignoreKw []string) string {
	prefix := fmt.Sprintf("[%s] [container=%s] ", time.Now().Format("2006-01-02T15:04:05Z07:00"), container)
	if hitIgnore(line, ignoreKw) {
		return prefix + line
	}
	if hitAny(line, errKw) {
		return prefix + redStart + line + redEnd
	}
	return prefix + line
}

// appendFile 追加写（O_CREATE|O_APPEND|O_WRONLY），目录不存在自动创建。
// 用于日志跟踪的持续增量落盘：保持文件句柄打开逐行写入，关闭时统一 flush。
func appendFile(path string) (*os.File, error) {
	if dir := filepath.Dir(path); dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
	}
	return os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
}

func hitIgnore(line string, ignoreKw []string) bool { return hitAny(line, ignoreKw) }

func hitAny(line string, kws []string) bool {
	ll := strings.ToLower(line)
	for _, kw := range kws {
		if kw != "" && strings.Contains(ll, strings.ToLower(kw)) {
			return true
		}
	}
	return false
}
