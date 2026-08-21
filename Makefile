# kubectl-check 构建与测试管理
# 用法: make <target>
#   make build      编译二进制到 ./bin/kubectl-check
#   make test       运行全部单元测试
#   make tidy       整理 go.mod / go.sum 依赖
#   make vet        静态检查
#   make fmt        格式化代码
#   make clean      清理编译产物
#   make install    安装到 GOBIN (kubectl-check)

BINARY      ?= kubectl-check
PKG         ?= kubecheck
BIN_DIR     ?= ./bin
OUT         ?= $(BIN_DIR)/$(BINARY)
GO          ?= go
PLATFORMS   ?= linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64

.PHONY: help build test tidy vet fmt clean install all $(PLATFORMS)

help: ## 显示帮助
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build: ## 编译二进制
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(OUT) .

test: ## 运行全部单元测试
	$(GO) test ./... -count=1

test-integration: ## 运行集成测试（需本地 kubeconfig 与目标集群，针对 dev/nginx）
	$(GO) test -tags=integration ./internal/checker/... -count=1 -v

tidy: ## 整理依赖
	$(GO) mod tidy

vet: ## 静态检查
	$(GO) vet ./...

fmt: ## 格式化代码
	$(GO) fmt ./...

clean: ## 清理编译产物
	@rm -rf $(BIN_DIR)

install: ## 安装到 GOBIN
	$(GO) install .

# 交叉编译: make all
$(PLATFORMS):
	@mkdir -p $(BIN_DIR)
	@OS=$(word 1,$(subst /, ,$@)); ARCH=$(word 2,$(subst /, ,$@)); \
	EXT=; [ "$$OS" = "windows" ] && EXT=".exe"; \
	echo "building $$OS/$$ARCH"; \
	GOOS=$$OS GOARCH=$$ARCH $(GO) build -o $(BIN_DIR)/$(BINARY)-$$OS-$$ARCH$$EXT .

all: $(PLATFORMS) ## 交叉编译多平台产物
