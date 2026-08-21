package recorder

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
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
