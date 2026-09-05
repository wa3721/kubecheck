//go:build integration

// 集成测试：连接当前集群的 dev 命名空间下 nginx deployment，进行真实校验演练。
// 默认不执行；运行方式：
//
//	make test-integration        （或 go test -tags=integration ./internal/checker/...）
//
// 需要本地 kubeconfig 可访问目标集群，且 dev/nginx deployment 已存在。
package checker

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"

	"kubecheck/internal/options"
)

// TestIntegrationDevNginx 对 dev 命名空间下的 nginx deployment 执行真实校验
func TestIntegrationDevNginx(t *testing.T) {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	cfgOverrides := &clientcmd.ConfigOverrides{}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loadingRules, cfgOverrides).ClientConfig()
	if err != nil {
		t.Fatalf("构建 kubeconfig 失败: %v", err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("构建 clientset 失败: %v", err)
	}

	// 确认 dev/nginx 存在
	if _, err := clientset.AppsV1().Deployments("dev").Get(context.Background(), "nginx",
		metav1.GetOptions{}); err != nil {
		t.Fatalf("dev/nginx deployment 不存在: %v", err)
	}

	opts := &options.Options{
		Namespace:             "dev",
		ResourceType:          "deployment",
		ResourceName:          "nginx",
		PodReadyTimeoutSec:    120,
		LogCheckTimeoutSec:    60,
		FeishuDedupWindowSec:  30,
		MaxRestart:            3,
		CheckPodStatus:        true,
		LogEnable:             true,
		LogErrorDir:           t.TempDir(),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	code := New(opts, clientset).Run(ctx)
	t.Logf("kubectl-check 对 dev/nginx 执行结束，退出码 = %d", code)
	// 集成测试只验证流程可正常跑通（不强制特定退出码，取决于集群真实状态）
	if code == CodeParam {
		t.Fatalf("参数/资源校验失败，退出码 %d", code)
	}
}
