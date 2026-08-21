package cmd

import (
	"testing"

	"kubecheck/internal/options"
	"github.com/spf13/cobra"
)

func TestParseArgs(t *testing.T) {
	// parseArgs 仅做格式/范围校验，默认值由 cobra flag 提供，测试需先填默认值
	withDefaults := func() *options.Options {
		return &options.Options{
			DeployReadyTimeoutSec: 300,
			PodReadyTimeoutSec:    120,
			LogCheckTimeoutSec:    60,
			LogErrorDir:           ".",
		}
	}
	c := &cobra.Command{}

	// deployment/name 形式
	opts := withDefaults()
	if err := parseArgs(c, []string{"deployment/myapp"}, opts); err != nil {
		t.Fatalf("parseArgs err: %v", err)
	}
	if opts.ResourceType != "deployment" || opts.ResourceName != "myapp" {
		t.Fatalf("got %s/%s", opts.ResourceType, opts.ResourceName)
	}

	// deployment name 形式
	opts2 := withDefaults()
	if err := parseArgs(c, []string{"deployment", "myapp2"}, opts2); err != nil {
		t.Fatalf("parseArgs (two args) err: %v", err)
	}
	if opts2.ResourceType != "deployment" || opts2.ResourceName != "myapp2" {
		t.Fatalf("got %s/%s", opts2.ResourceType, opts2.ResourceName)
	}

	// 不支持的资源类型
	opts3 := withDefaults()
	if err := parseArgs(c, []string{"statefulset/myapp"}, opts3); err == nil {
		t.Fatal("should reject non-deployment resource")
	}

	// 格式错误
	opts4 := withDefaults()
	if err := parseArgs(c, []string{"myapp"}, opts4); err == nil {
		t.Fatal("should reject malformed resource arg")
	}
}

func TestParseArgsTimeout(t *testing.T) {
	c := &cobra.Command{}
	// pod-ready-timeout<=0 应被拒绝
	opts := &options.Options{DeployReadyTimeoutSec: 300, PodReadyTimeoutSec: 0, LogCheckTimeoutSec: 60, LogErrorDir: "."}
	if err := parseArgs(c, []string{"deployment/myapp"}, opts); err == nil {
		t.Fatal("pod-ready-timeout<=0 should be rejected")
	}
	// 合法配置应通过
	opts.PodReadyTimeoutSec = 120
	if err := parseArgs(c, []string{"deployment/myapp"}, opts); err != nil {
		t.Fatalf("valid config should pass: %v", err)
	}
	// deploy-ready-timeout 与 log-check-timeout 允许为 0（禁用），但不可为负
	opts.DeployReadyTimeoutSec = -1
	if err := parseArgs(c, []string{"deployment/myapp"}, opts); err == nil {
		t.Fatal("deploy-ready-timeout<0 should be rejected")
	}
	opts.DeployReadyTimeoutSec = 0
	opts.LogCheckTimeoutSec = -1
	if err := parseArgs(c, []string{"deployment/myapp"}, opts); err == nil {
		t.Fatal("log-check-timeout<0 should be rejected")
	}
	opts.LogCheckTimeoutSec = 0
	if err := parseArgs(c, []string{"deployment/myapp"}, opts); err != nil {
		t.Fatalf("zero timeouts should pass: %v", err)
	}
}

func TestNewCheckCommand(t *testing.T) {
	cmd := NewCheckCommand()
	if cmd.Use == "" {
		t.Fatal("command Use empty")
	}
	flags := cmd.Flags()
	drt, _ := flags.GetInt("deploy-ready-timeout")
	if drt != 300 {
		t.Fatalf("default deploy-ready-timeout = %d, want 300", drt)
	}
	prt, _ := flags.GetInt("pod-ready-timeout")
	if prt != 120 {
		t.Fatalf("default pod-ready-timeout = %d, want 120", prt)
	}
	lct, _ := flags.GetInt("log-check-timeout")
	if lct != 60 {
		t.Fatalf("default log-check-timeout = %d, want 60", lct)
	}
}
