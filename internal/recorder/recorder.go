package recorder

import (
	"bufio"
	"context"
	"fmt"
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
// 逐行增量追加写至 {namespace}-{podName}-{date}.log，错误行红色标注。
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

// TrackLog 在 ctx 生命周期内实时跟踪 Pod 日志（Follow=true）：
// 逐行增量追加写至 {ns}-{podName}-{date}.log，命中错误关键字时标记 errorHit。
// 返回 (lines 累计行数, errorHit 是否命中错误关键字, err)。
//
// 持续增量写入：日志流每来一行即写一行，进程被中断/超时结束前由文件句柄关闭
// 保证已读日志全部落盘。容器在 ContainerCreating / 未启动时 GetLogs 必然失败，
// 故先在 ctx 内轮询等待至少一个容器进入 Running 再开始流式跟踪，
// 避免"开流即失败→误判无错误日志"。
func (r *LogErrorRecorder) TrackLog(ctx context.Context, clientset kubernetes.Interface, pod *corev1.Pod, errKw, ignoreKw []string, tail int) (int, bool, error) {
	var (
		errorHit bool
		count    int
	)

	// 等待容器进入 Running：ContainerCreating 阶段开流会失败
	pod = r.waitForContainerRunning(ctx, clientset, pod)
	if ctx.Err() != nil {
		return 0, false, nil
	}

	// 打开增量日志文件（追加模式，持续写入）
	path := r.FilePath(pod.Namespace, pod.Name)
	f, err := appendFile(path)
	if err != nil {
		return 0, false, fmt.Errorf("打开日志文件 %s 失败: %w", path, err)
	}
	defer f.Close()

	for _, cs := range pod.Status.ContainerStatuses {
		// 跳过尚未 Running 的容器（如仍 ContainerCreating）
		if cs.State.Running == nil {
			continue
		}
		opts := &corev1.PodLogOptions{
			Container:  cs.Name,
			Follow:     true,
			Timestamps: true,
		}
		if tail > 0 {
			opts.TailLines = int64Ptr(int64(tail))
		}
		stream, err := clientset.CoreV1().Pods(pod.Namespace).GetLogs(pod.Name, opts).Stream(ctx)
		if err != nil {
			if ctx.Err() == nil {
				fmt.Printf("[WARN] 跟踪 %s/%s 容器 %s 日志流失败: %v\n", pod.Namespace, pod.Name, cs.Name, err)
			}
			continue
		}
		sc := bufio.NewScanner(stream)
		for sc.Scan() {
			line := sc.Text()
			if hitAny(line, errKw) && !hitIgnore(line, ignoreKw) {
				errorHit = true
			}
			if _, werr := fmt.Fprintf(f, "%s\n", colorizeErrorLine(line, cs.Name, errKw, ignoreKw)); werr != nil {
				return count, errorHit, werr
			}
			count++
		}
		_ = stream.Close()
		if ctx.Err() != nil {
			break
		}
	}

	// 已读日志已随文件句柄关闭全部落盘（增量写，不依赖是否命中错误）
	r.mu.Lock()
	r.recorded[podKey(pod.Namespace, pod.Name)] = path
	r.mu.Unlock()
	return count, errorHit, nil
}

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
