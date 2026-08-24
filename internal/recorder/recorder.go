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
)

// 错误行红色标注（ANSI 转义码）
const (
	redStart = "\x1b[31m"
	redEnd   = "\x1b[0m"
)

// LogErrorRecorder 错误日志收集层：以 Follow 模式持续跟踪 Pod 日志，
// 命中错误关键字时才将"启动到报错"的日志落盘至 {namespace}-{podName}-{date}.log，
// 错误行红色标注；未命中错误且容器未报错退出的应用不产生日志文件。
type LogErrorRecorder struct {
	dir string

	mu       sync.Mutex
	recorded map[string]string // podKey(ns/name) -> 已落盘文件路径
}

func New(dir string) *LogErrorRecorder {
	return &LogErrorRecorder{
		dir:      dir,
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

// TrackLog 在 ctx 生命周期内实时跟踪 Pod 日志：
// 命中错误关键字时才落盘——首次命中时把"从日志起点（含 --log-tail 回溯）到
// 报错行"的缓冲日志一次性写入文件，此后多次命中错误则逐行增量追加更新；
// 未命中错误且容器未报错退出的应用不产生日志文件。
// 返回 (lines 累计读取行数, errorHit 是否命中错误关键字, err)。
//
// 落盘条件（与检查结论联动）：
//   - 命中错误关键字（errorHit）-> 落盘，错误行红色标注
//   - 容器异常退出（exit code != 0，即便日志未命中关键字）-> 落盘缓冲日志保留现场
//   - 正常应用（未命中错误、容器未报错退出）-> 不落盘，不产生文件
//
// 流式策略（兼顾 CrashLoopBackOff 等快速退出场景）：
//   - 容器 Running：优先 Follow 持续跟踪；开流失败（容器快速退出竞态）时
//     降级为非 Follow 一次性拉取历史日志
//   - 容器已 Terminated / 等待重启（有 containerID，即曾启动过）：非 Follow 拉取
//   - 容器从未启动（ContainerCreating，无 containerID）：跳过
func (r *LogErrorRecorder) TrackLog(ctx context.Context, clientset kubernetes.Interface, pod *corev1.Pod, errKw, ignoreKw []string, tail int) (int, bool, error) {
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
				fmt.Printf("[WARN] 跟踪 %s/%s 容器 %s 日志流失败: %v\n", pod.Namespace, pod.Name, cs.Name, err)
			}
			continue
		}
		sc := bufio.NewScanner(stream)
		for sc.Scan() {
			line := sc.Text()
			count++
			if hitAny(line, errKw) && !hitIgnore(line, ignoreKw) {
				errorHit = true
				// 首次命中错误：缓冲日志 + 本行一次性落盘；之后增量追加
				if err := flushBuf(); err != nil {
					_ = stream.Close()
					return count, errorHit, err
				}
				if _, werr := fmt.Fprintf(f, "%s\n", colorizeErrorLine(line, cs.Name, errKw, ignoreKw)); werr != nil {
					_ = stream.Close()
					return count, errorHit, werr
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
	if f == nil && buf.len() > 0 {
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
