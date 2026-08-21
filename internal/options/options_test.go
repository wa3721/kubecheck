package options

import (
	"testing"
	"time"
)

func TestDeployReadyTimeoutSeconds(t *testing.T) {
	o := &Options{DeployReadyTimeoutSec: 300}
	if got := o.DeployReadyTimeout(); got != 300*time.Second {
		t.Fatalf("DeployReadyTimeout() = %v, want %v", got, 300*time.Second)
	}
}

func TestPodReadyTimeoutSeconds(t *testing.T) {
	o := &Options{PodReadyTimeoutSec: 120}
	if got := o.PodReadyTimeout(); got != 120*time.Second {
		t.Fatalf("PodReadyTimeout() = %v, want %v", got, 120*time.Second)
	}
}

func TestLogCheckTimeoutSeconds(t *testing.T) {
	o := &Options{LogCheckTimeoutSec: 60}
	if got := o.LogCheckTimeout(); got != 60*time.Second {
		t.Fatalf("LogCheckTimeout() = %v, want %v", got, 60*time.Second)
	}
}

func TestFeishuDedupWindowSeconds(t *testing.T) {
	o := &Options{FeishuDedupWindowSec: 30}
	if got := o.FeishuDedupWindow(); got != 30*time.Second {
		t.Fatalf("FeishuDedupWindow() = %v, want %v", got, 30*time.Second)
	}
}
