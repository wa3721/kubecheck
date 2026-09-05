package recorder

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func TestColorizeErrorLine(t *testing.T) {
	redStart := "\x1b[31m"
	redEnd := "\x1b[0m"

	// 错误行标红
	out := colorizeErrorLine("something error happened", "app", []string{"error"}, nil)
	if !strings.Contains(out, redStart+"something error happened"+redEnd) {
		t.Fatalf("error line not red-tagged: %q", out)
	}

	// 忽略行不标红
	out = colorizeErrorLine("benign error log", "app", []string{"error"}, []string{"benign"})
	if strings.Contains(out, redStart) {
		t.Fatalf("ignored line should not be red: %q", out)
	}
	if !strings.Contains(out, "benign error log") {
		t.Fatalf("ignored line content missing: %q", out)
	}

	// 普通行不标红
	out = colorizeErrorLine("normal log", "app", []string{"error"}, nil)
	if strings.Contains(out, redStart) {
		t.Fatalf("normal line should not be red: %q", out)
	}
	if !strings.HasPrefix(out, "[") {
		t.Fatalf("line should have timestamp+container prefix: %q", out)
	}
}

func TestFilePathAndAppend(t *testing.T) {
	dir := t.TempDir()
	r := New(dir, true, false)
	p := r.FilePath("delta", "myapp")
	if !strings.HasSuffix(p, "delta-myapp-"+time.Now().Format("20060102")+".log") {
		t.Fatalf("filepath format wrong: %s", p)
	}

	// 追加写：写两次，内容为两次合并（对应 TrackLog 持续增量落盘）
	f, err := appendFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("first\n")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	f2, err := appendFile(p)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f2.Write([]byte("second\n")); err != nil {
		t.Fatal(err)
	}
	if err := f2.Close(); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(p)
	if string(data) != "first\nsecond\n" {
		t.Fatalf("append failed, got %q", string(data))
	}

	// 子目录自动创建
	sub := filepath.Join(dir, "nested", "f.log")
	f3, err := appendFile(sub)
	if err != nil {
		t.Fatal(err)
	}
	f3.Close()
	if _, err := os.Stat(sub); err != nil {
		t.Fatalf("nested dir not created: %v", err)
	}
}

// lineRing 环形缓冲：顺序保留、满容量丢弃最旧、drain 后清空
func TestLineRing(t *testing.T) {
	ring := newLineRing(3)
	if ring.len() != 0 {
		t.Fatalf("empty ring len = %d", ring.len())
	}

	// 未满：按序保留
	ring.add("a")
	ring.add("b")
	ring.add("c")
	if got := ring.drain(); !equalStrings(got, []string{"a", "b", "c"}) {
		t.Fatalf("drain before full = %v", got)
	}
	if ring.len() != 0 {
		t.Fatalf("drain should clear, len = %d", ring.len())
	}

	// 超过容量：丢弃最旧，保留最近 N 行（FIFO）
	ring.add("1")
	ring.add("2")
	ring.add("3")
	ring.add("4")
	ring.add("5")
	if got := ring.drain(); !equalStrings(got, []string{"3", "4", "5"}) {
		t.Fatalf("drain after overflow = %v", got)
	}
}

// containerExited：任一容器 Terminated 且退出码非 0 才判定为退出
func TestContainerExited(t *testing.T) {
	{
		code, exited := containerExited(nil)
		if exited || code != 0 {
			t.Fatalf("nil pod should not be exited, got (%d, %v)", code, exited)
		}
	}
	{
		p := &corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}},
		}}}
		if _, exited := containerExited(p); exited {
			t.Fatalf("running container should not be exited")
		}
	}
	{
		p := &corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 1}}},
		}}}
		code, exited := containerExited(p)
		if !exited || code != 1 {
			t.Fatalf("terminated(exit=1) should be exited, got (%d, %v)", code, exited)
		}
	}
	{
		p := &corev1.Pod{Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{
			{State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}}},
		}}}
		if _, exited := containerExited(p); exited {
			t.Fatalf("terminated(exit=0) should NOT be treated as abnormal exit")
		}
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestArbBreaker 熔断器：连续 3 次失败后熔断，成功重置计数，熔断后不再放行
func TestArbBreaker(t *testing.T) {
	b := newArbBreaker()
	if !b.allow() {
		t.Fatal("初始应允许调用")
	}
	if b.recordFail() {
		t.Fatal("第 1 次失败不应熔断")
	}
	if b.recordFail() {
		t.Fatal("第 2 次失败不应熔断")
	}
	if !b.allow() {
		t.Fatal("未达阈值不应熔断")
	}
	if !b.recordFail() {
		t.Fatal("第 3 次失败应触发熔断")
	}
	if b.allow() {
		t.Fatal("熔断后不应允许调用")
	}
	// 熔断后继续失败不再重复报告
	if b.recordFail() {
		t.Fatal("已熔断后不应重复触发")
	}
	// 熔断不可恢复（本工具生命周期内端点持续不可达即保持熔断语义）
	if b.allow() {
		t.Fatal("熔断应保持")
	}
}

// TestArbBreakerReset 失败计数被成功调用重置
func TestArbBreakerReset(t *testing.T) {
	b := newArbBreaker()
	b.recordFail()
	b.recordFail()
	b.recordSuccess() // 成功一次重置
	if b.recordFail() {
		t.Fatal("重置后第 1 次失败不应熔断")
	}
	if !b.allow() {
		t.Fatal("重置后未熔断应继续允许调用")
	}
}

// TestAllTrue 降级判定结果生成
func TestAllTrue(t *testing.T) {
	res := allTrue(3)
	if len(res) != 3 {
		t.Fatalf("len = %d, want 3", len(res))
	}
	for i, v := range res {
		if !v {
			t.Fatalf("res[%d] 应为 true", i)
		}
	}
}

// ---------- LLM 仲裁路径（trackLogWithJudge / snapshotFull）----------
//
// fake clientset 的 GetLogs 固定返回 "fake logs"：关键字取 "fake" 即可构造命中场景；
// 通过 reactor 捕获 GetLogs 的 PodLogOptions 以断言 SinceTime 透传。

// fakeJudge 可编程仲裁 fake：allTrue 控制判定结果，err 模拟调用失败
type fakeJudge struct {
	enabled bool
	allTrue bool
	err     error
	calls   int
}

func (f *fakeJudge) Enabled() bool                { return f.enabled }
func (f *fakeJudge) Timeout() time.Duration       { return time.Second }
func (f *fakeJudge) Judge(_ context.Context, lines []string) ([]bool, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	res := make([]bool, len(lines))
	for i := range res {
		res[i] = f.allTrue
	}
	return res, nil
}

// makeLogPod 构造有日志可拉（容器 Running）的 Pod
func makeLogPod(name string) *corev1.Pod {
	started := metav1.NewTime(time.Now().Add(-time.Hour))
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:        "c",
				Ready:       true,
				ContainerID: "docker://c0",
				State:       corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: started}},
			}},
		},
	}
}

// captureLogOptions 捕获 GetLogs 调用携带的 PodLogOptions（断言 SinceTime 透传）
func captureLogOptions(t *testing.T, cs *fake.Clientset) *[]*corev1.PodLogOptions {
	t.Helper()
	captured := &[]*corev1.PodLogOptions{}
	cs.PrependReactor("get", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		if action.GetSubresource() != "log" {
			return false, nil, nil
		}
		if g, ok := action.(interface{ GetValue() interface{} }); ok {
			if opts, ok := g.GetValue().(*corev1.PodLogOptions); ok {
				*captured = append(*captured, opts)
			}
		}
		return false, nil, nil
	})
	return captured
}

// TestTrackLogJudgeTrueSnapshot LLM 判定真错误 -> SinceTime 全量拉取覆盖写快照 + hit=true
func TestTrackLogJudgeTrueSnapshot(t *testing.T) {
	dir := t.TempDir()
	r := New(dir, true, false)
	pod := makeLogPod("p1")
	cs := fake.NewSimpleClientset(pod)
	logOpts := captureLogOptions(t, cs)
	j := &fakeJudge{enabled: true, allTrue: true}

	// fake GetLogs 返回 "fake logs"，关键字 "fake" 命中后交由仲裁
	_, hit, err := r.TrackLog(context.Background(), cs, pod, j, []string{"fake"}, nil, 100)
	if err != nil {
		t.Fatalf("TrackLog: %v", err)
	}
	if !hit {
		t.Fatal("真错误应置 hit=true")
	}
	if j.calls == 0 {
		t.Fatal("仲裁器应被调用")
	}

	path := r.FilePath("default", "p1")
	data, rerr := os.ReadFile(path)
	if rerr != nil {
		t.Fatalf("真错误应产生全量快照文件: %v", rerr)
	}
	if !strings.Contains(string(data), "fake logs") {
		t.Fatalf("快照内容缺失: %q", data)
	}
	if r.RecordedPath("default", "p1") != path {
		t.Fatal("recorded 应记录快照路径")
	}

	// 全量快照拉取必须携带 SinceTime（容器启动时刻）
	sawSince := false
	for _, o := range *logOpts {
		if o.SinceTime != nil {
			sawSince = true
		}
	}
	if !sawSince {
		t.Fatal("全量拉取应携带 SinceTime")
	}
}

// TestTrackLogJudgeFalseNoFile 全部假错误 -> 不落盘、不告警（hit=false）
func TestTrackLogJudgeFalseNoFile(t *testing.T) {
	dir := t.TempDir()
	r := New(dir, true, false)
	pod := makeLogPod("p1")
	cs := fake.NewSimpleClientset(pod)
	j := &fakeJudge{enabled: true, allTrue: false}

	_, hit, err := r.TrackLog(context.Background(), cs, pod, j, []string{"fake"}, nil, 100)
	if err != nil {
		t.Fatalf("TrackLog: %v", err)
	}
	if hit {
		t.Fatal("假错误不应置 hit")
	}
	if _, serr := os.Stat(r.FilePath("default", "p1")); !os.IsNotExist(serr) {
		t.Fatal("假错误不应产生日志文件")
	}
	if r.RecordedPath("default", "p1") != "" {
		t.Fatal("假错误不应记录快照路径")
	}
}

// TestTrackLogConsoleOutput --log-console（console=true）：追踪流读到的日志行
// 逐行实时输出到控制台（原始行，同 kubectl logs -f）；console=false 静默。
// 覆盖双路径：LLM 仲裁 / 关键字即真。
func TestTrackLogConsoleOutput(t *testing.T) {
	pod := makeLogPod("p1")
	errKw := []string{"fake"}

	// 路径1：LLM 仲裁路径 console=true -> 输出原始日志行
	{
		r := New(t.TempDir(), false, true)
		cs := fake.NewSimpleClientset(pod)
		j := &fakeJudge{enabled: true, allTrue: true}

		old := os.Stdout
		rd, wr, _ := os.Pipe()
		os.Stdout = wr
		done := make(chan string)
		go func() {
			b, _ := io.ReadAll(rd)
			done <- string(b)
		}()
		_, hit, err := r.TrackLog(context.Background(), cs, pod, j, errKw, nil, 100)
		wr.Close()
		os.Stdout = old
		out := <-done
		if err != nil || !hit {
			t.Fatalf("TrackLog(judge): hit=%v err=%v", hit, err)
		}
		if !strings.Contains(out, "fake logs") {
			t.Fatalf("console=true should print raw log lines, got: %q", out)
		}
	}

	// 路径2：关键字路径 console=true -> 输出原始日志行
	{
		r := New(t.TempDir(), false, true)
		cs := fake.NewSimpleClientset(pod)

		old := os.Stdout
		rd, wr, _ := os.Pipe()
		os.Stdout = wr
		done := make(chan string)
		go func() {
			b, _ := io.ReadAll(rd)
			done <- string(b)
		}()
		_, hit, err := r.TrackLog(context.Background(), cs, pod, nil, errKw, nil, 100)
		wr.Close()
		os.Stdout = old
		out := <-done
		if err != nil || !hit {
			t.Fatalf("TrackLog(keyword): hit=%v err=%v", hit, err)
		}
		if !strings.Contains(out, "fake logs") {
			t.Fatalf("console=true should print raw log lines, got: %q", out)
		}
	}

	// console=false -> 静默（无日志行输出）
	{
		r := New(t.TempDir(), false, false)
		cs := fake.NewSimpleClientset(pod)

		old := os.Stdout
		rd, wr, _ := os.Pipe()
		os.Stdout = wr
		done := make(chan string)
		go func() {
			b, _ := io.ReadAll(rd)
			done <- string(b)
		}()
		_, _, err := r.TrackLog(context.Background(), cs, pod, nil, errKw, nil, 100)
		wr.Close()
		os.Stdout = old
		out := <-done
		if err != nil {
			t.Fatalf("TrackLog(silent): %v", err)
		}
		if strings.Contains(out, "fake logs") {
			t.Fatalf("console=false should be silent, got: %q", out)
		}
	}
}

// TestTrackLogDumpDisabled --log-dump 关闭（dump=false）：检测/errorHit 判定照常，
// 但两个路径（LLM 仲裁真错误 / 关键字即真）均不产生日志文件、不登记路径
func TestTrackLogDumpDisabled(t *testing.T) {
	// 路径1：LLM 仲裁判定真错误 -> hit=true 但不落盘
	{
		dir := t.TempDir()
		r := New(dir, false, false)
		pod := makeLogPod("p1")
		cs := fake.NewSimpleClientset(pod)
		j := &fakeJudge{enabled: true, allTrue: true}

		_, hit, err := r.TrackLog(context.Background(), cs, pod, j, []string{"fake"}, nil, 100)
		if err != nil {
			t.Fatalf("TrackLog(judge): %v", err)
		}
		if !hit {
			t.Fatal("dump=false 不影响 errorHit 判定，真错误应置 hit=true")
		}
		if _, serr := os.Stat(r.FilePath("default", "p1")); !os.IsNotExist(serr) {
			t.Fatal("dump=false 时不应产生日志文件")
		}
		if r.RecordedPath("default", "p1") != "" {
			t.Fatal("dump=false 时不应登记落盘路径")
		}
	}
	// 路径2：未启用仲裁（关键字即真）-> errorHit=true 但不落盘
	{
		dir := t.TempDir()
		r := New(dir, false, false)
		pod := makeLogPod("p2")
		cs := fake.NewSimpleClientset(pod)

		_, hit, err := r.TrackLog(context.Background(), cs, pod, nil, []string{"fake"}, nil, 100)
		if err != nil {
			t.Fatalf("TrackLog(keyword): %v", err)
		}
		if !hit {
			t.Fatal("dump=false 不影响关键字命中判定，应置 hit=true")
		}
		if _, serr := os.Stat(r.FilePath("default", "p2")); !os.IsNotExist(serr) {
			t.Fatal("dump=false 时不应产生日志文件")
		}
		if r.RecordedPath("default", "p2") != "" {
			t.Fatal("dump=false 时不应登记落盘路径")
		}
	}
}

// TestTrackLogJudgeErrorDegrades 判定调用失败 -> 降级为真错误（落盘+hit）
func TestTrackLogJudgeErrorDegrades(t *testing.T) {
	dir := t.TempDir()
	r := New(dir, true, false)
	pod := makeLogPod("p1")
	cs := fake.NewSimpleClientset(pod)
	j := &fakeJudge{enabled: true, err: errors.New("llm down")}

	_, hit, err := r.TrackLog(context.Background(), cs, pod, j, []string{"fake"}, nil, 100)
	if err != nil {
		t.Fatalf("TrackLog: %v", err)
	}
	if !hit {
		t.Fatal("降级应视为真错误（hit=true）")
	}
	if _, serr := os.Stat(r.FilePath("default", "p1")); serr != nil {
		t.Fatal("降级应落盘保留现场")
	}
}

// TestTrackLogExitSnapshotWithJudge 容器异常退出且无真错误命中 ->
// 无条件全量快照保留现场，但不置 hit（与现行退出路径语义一致）
func TestTrackLogExitSnapshotWithJudge(t *testing.T) {
	dir := t.TempDir()
	r := New(dir, true, false)
	pod := makeLogPod("p1")
	started := metav1.NewTime(time.Now().Add(-time.Hour))
	// 追加一个异常退出的 sidecar 容器（主容器仍 Running，保证流式路径可走通）
	pod.Status.ContainerStatuses = append(pod.Status.ContainerStatuses, corev1.ContainerStatus{
		Name:        "sidecar",
		ContainerID: "docker://c1",
		State: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, StartedAt: started},
		},
	})
	cs := fake.NewSimpleClientset(pod)
	j := &fakeJudge{enabled: true, allTrue: false}

	// errKw 不命中 "fake logs"：验证退出路径不经仲裁
	_, hit, err := r.TrackLog(context.Background(), cs, pod, j, []string{"nomatch"}, nil, 100)
	if err != nil {
		t.Fatalf("TrackLog: %v", err)
	}
	if hit {
		t.Fatal("退出兜底快照不应置 hit（exit 已由 checker 单独告警）")
	}
	if _, serr := os.Stat(r.FilePath("default", "p1")); serr != nil {
		t.Fatal("容器异常退出应无条件快照保留现场")
	}
}

// TestTrackLogKeywordPathWithJudgeDisabled 未启用仲裁（Enabled=false）-> 走现行关键字即真路径
func TestTrackLogKeywordPathWithJudgeDisabled(t *testing.T) {
	dir := t.TempDir()
	r := New(dir, true, false)
	pod := makeLogPod("p1")
	cs := fake.NewSimpleClientset(pod)
	j := &fakeJudge{enabled: false}

	_, hit, err := r.TrackLog(context.Background(), cs, pod, j, []string{"fake"}, nil, 100)
	if err != nil {
		t.Fatalf("TrackLog: %v", err)
	}
	if !hit {
		t.Fatal("未启用仲裁应保持关键字即真（hit=true）")
	}
	if j.calls != 0 {
		t.Fatal("未启用仲裁不应调用 Judge")
	}
	if _, serr := os.Stat(r.FilePath("default", "p1")); serr != nil {
		t.Fatal("关键字即真路径应落盘")
	}
}

// TestTrackLogNilJudge judge 为 nil -> 同未启用（关键字即真）
func TestTrackLogNilJudge(t *testing.T) {
	dir := t.TempDir()
	r := New(dir, true, false)
	pod := makeLogPod("p1")
	cs := fake.NewSimpleClientset(pod)

	_, hit, err := r.TrackLog(context.Background(), cs, pod, nil, []string{"fake"}, nil, 100)
	if err != nil {
		t.Fatalf("TrackLog: %v", err)
	}
	if !hit {
		t.Fatal("nil judge 应走关键字即真路径")
	}
}

// TestContainerStartTime 全量拉取起点：Running 取 StartedAt，
// 已退出取 LastTerminationState.StartedAt，均不可得回退创建时间；多容器取最早
func TestContainerStartTime(t *testing.T) {
	running := metav1.NewTime(time.Now().Add(-2 * time.Hour))
	lastTerm := metav1.NewTime(time.Now().Add(-1 * time.Hour))
	created := metav1.NewTime(time.Now().Add(-3 * time.Hour))

	// 仅 Running 容器
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", CreationTimestamp: created},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: running}},
		}}},
	}
	if !containerStartTime(p).Time.Equal(running.Time) {
		t.Fatalf("Running 容器应取 StartedAt，got %v", containerStartTime(p))
	}

	// 已退出容器：取 LastTerminationState.StartedAt
	p2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", CreationTimestamp: created},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{
			LastTerminationState: corev1.ContainerState{
				Terminated: &corev1.ContainerStateTerminated{StartedAt: lastTerm},
			},
		}}},
	}
	if !containerStartTime(p2).Time.Equal(lastTerm.Time) {
		t.Fatalf("已退出容器应取 LastTerminationState.StartedAt，got %v", containerStartTime(p2))
	}

	// 均不可得：回退创建时间
	p3 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "p", CreationTimestamp: created},
		Status:     corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "c"}}},
	}
	if !containerStartTime(p3).Time.Equal(created.Time) {
		t.Fatalf("应回退创建时间，got %v", containerStartTime(p3))
	}
}

// TestTruncFile trunc=true 覆盖旧快照，trunc=false 追加（多容器合并）
func TestTruncFile(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "ns-pod.log")

	f, err := truncFile(p, true)
	if err != nil {
		t.Fatal(err)
	}
	f.WriteString("first\n")
	f.Close()

	// 追加（后续容器）
	f2, err := truncFile(p, false)
	if err != nil {
		t.Fatal(err)
	}
	f2.WriteString("second\n")
	f2.Close()
	data, _ := os.ReadFile(p)
	if string(data) != "first\nsecond\n" {
		t.Fatalf("append failed, got %q", data)
	}

	// 覆盖（重新快照）
	f3, err := truncFile(p, true)
	if err != nil {
		t.Fatal(err)
	}
	f3.WriteString("fresh\n")
	f3.Close()
	data, _ = os.ReadFile(p)
	if string(data) != "fresh\n" {
		t.Fatalf("truncate failed, got %q", data)
	}
}
