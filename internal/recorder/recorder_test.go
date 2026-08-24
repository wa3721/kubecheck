package recorder

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
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
	r := New(dir)
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
