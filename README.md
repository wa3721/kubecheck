# kubectl-check

`kubectl-check` 是 `kubectl` 插件，用于部署后的自动化校验：**无整体超时控制**，分两级独立检查 Deployment 就绪情况：

- **第一级**：Deployment informer 在 `--deploy-ready-timeout` 内检查 Deployment 是否满足就绪条件（默认 `Available`），且要求 `status.observedGeneration >= metadata.generation`（即已观察到最新 spec、本次发布/回滚已完成）。超时未就绪则**发送告警（优先飞书，无飞书降级控制台）后直接退出，不再继续追踪 Pod**（退出码 2）。
- **第二级**：Pod informer 在 `--pod-ready-timeout` 内追踪对应 Pod 状态，同时日志跟踪在 `--log-check-timeout` 内执行，两者**并行**。优先级：**Pod 未就绪 > 日志报错**。
  - 场景1：Pod 在超时内就绪且无错误日志 → 正常退出（码 0）。
  - 场景2：Pod 超时未就绪且无错误日志命中 → 发送告警，程序**异常退出**（码 3）。
  - 场景3：Pod 在超时内已就绪但日志命中错误关键字 → 标记 Pod，日志检查超时后将该 Pod 从启动到错误日志**全部落盘**，并发送提醒「xx pod 存在错误日志，但 pod 已就绪，需要检查」（不退出，码 0）。

基于纯官方生态实现：**cobra** 命令行框架 + **client-go** informer 事件驱动（复刻原生 `kubectl wait`）。

---

## 特性

- **两级独立检查，无整体超时**：三段超时（`--deploy-ready-timeout` / `--pod-ready-timeout` / `--log-check-timeout`）彼此独立，无顶层兜底超时。
- **第一级 Deployment 就绪检查**：监听 Deployment Condition（默认 `Available`），超时未就绪则告警后直接退出（退出码 2）。
- **第二级 Pod 状态 + 日志并行追踪**：Pod 状态在 `--pod-ready-timeout` 内轮询，日志在 `--log-check-timeout` 内实时跟踪，互不阻塞。
- **优先级：Pod 未就绪 > 日志报错**：
  - 存在「Pod 未就绪 / 重启超限」→ 场景2，告警并异常退出（码 3）；
  - 全部就绪但存在错误日志 → 场景3，落盘完整日志 + 发送「需要检查」提醒（码 0）。
- **错误日志落盘（完整快照覆盖写）**：将 Pod 从容器启动到报错时刻的**完整日志**覆盖写入 `{namespace}-{podName}-{date}.log`，错误行 ANSI 红色标注。
- **飞书告警**：优先飞书；**未配置 Webhook 降级控制台**，告警事件不静默丢弃。
- **主动退出**：收到 `SIGINT`/`SIGTERM` 时优雅退出（退出码 130），不误报为业务错误。

---

## 安装与构建

```bash
make tidy      # 整理依赖
make build     # 编译二进制到 ./bin/kubectl-check
make test      # 运行全部单元测试
make all       # 交叉编译多平台产物
make install   # 安装到 GOBIN
```

依赖：`github.com/spf13/cobra`、`k8s.io/client-go`（版本见 `go.mod`）。

---

## 使用方式

作为 kubectl 插件使用（二进制命名为 `kubectl-check` 并放入 `PATH`）：

```bash
# 基本用法：检查 delta 命名空间下 deployment/my-app（三级超时独立控制）
kubectl check -n delta deployment/my-app

# 自定义三段独立超时（秒级）
kubectl check -n delta deployment/my-app \
  --deploy-ready-timeout=300 --pod-ready-timeout=120 --log-check-timeout=60

# 启用错误日志落盘与飞书告警
kubectl check -n delta deployment/my-app \
  --log-error-dir=./errlogs \
  --feishu-webhook=https://open.feishu.cn/open-apis/bot/v2/hook/xxx \
  --feishu-secret=yyy

# 自定义日志关键字与忽略规则
kubectl check -n delta deployment/my-app \
  --log-err-keywords=error,panic,fatal \
  --log-ignore-keywords="level=debug"
```

> 资源参数支持 `deployment/my-app` 与 `deployment my-app` 两种 kubectl 风格。

---

## 参数说明

| 参数 | 作用 | 默认值 |
| --- | --- | --- |
| `-n / --namespace` | 目标命名空间 | `default` |
| 位置参数 `类型/名称` | 监控资源（当前仅 `deployment`） | 必传 |
| `--for` | 等待的 Deployment Condition | `Available` |
| `--deploy-ready-timeout` | 第一级：Deployment 就绪超时（秒），`<=0` 禁用 | `300` |
| `--pod-ready-timeout` | 第二级：Pod 状态就绪超时（秒） | `120` |
| `--log-check-timeout` | 第二级：日志跟踪超时（秒），`<=0` 禁用 | `60` |
| `--max-restart` | Pod 最大允许重启次数 | `0` |
| `--check-pod-status` | Pod 状态强校验开关 | `true` |
| `--log-enable` | 实时日志监控开关 | `true` |
| `--log-err-keywords` | 日志异常关键字（逗号分隔） | `error,panic,fatal,exception,crash` |
| `--log-ignore-keywords` | 忽略的无害日志关键字（逗号分隔） | 空 |
| `--log-tail` | 无启动时间时的回溯行数 | `100` |
| `--log-error-dir` | 错误日志落盘目录（文件：`namespace-podname-date.log`） | `.` |
| `--feishu-webhook` | 飞书机器人 Webhook，非空启用告警 | 空 |
| `--feishu-secret` | 飞书签名密钥（HMAC-SHA256） | 空 |
| `--feishu-dedup-window` | 同 Pod+同事件告警去重窗口（秒） | `30` |
| `-v / --verbose` | 详细日志输出 | `false` |
| `-h / --help` | 帮助文档 | - |

---

## 退出码

| 退出码 | 含义 |
| --- | --- |
| `0` | 正常：场景1（Pod 就绪且无错误日志）或场景3（Pod 就绪但存在错误日志，已落盘并提醒） |
| `1` | 参数解析错误、目标资源不存在 |
| `2` | Deployment 在就绪超时内未满足就绪条件（告警后直接退出，不继续追踪 Pod） |
| `3` | 场景2：Pod 超时未就绪 / 重启超限（无错误日志命中），异常退出 |
| `130` | 收到中断信号（Ctrl+C / SIGTERM）主动退出 |

---

## 错误日志落盘（场景3）

- 文件命名：`{namespace}-{podName}-{date}.log`（`date` 格式 `20060102`，跨天自动切换），目录由 `--log-error-dir` 指定。
- 记录内容：命中错误关键字时，以容器启动时间为起点（`State.Running.StartedAt`；已退出取 `LastTerminationState.Terminated.StartedAt`；均不可得回退 Pod 创建时间）拉取从启动到当前的**全量日志**。
- 错误行用 ANSI 红色转义码标注，忽略规则命中的行不标红；同 Pod 后续新错误覆盖写整份最新快照。

---

## 项目结构

```
main.go                      入口，调用 cmd.Execute()
internal/cmd/                 cobra 命令、参数解析、客户端构建
internal/options/             Options 参数定义（三段独立超时秒级转换）
internal/checker/             核心编排：第一级就绪等待 / 第二级 Pod+日志并行 / 场景判定 / 落盘
internal/recorder/            错误日志快照覆盖写 + 红色标注 + 容器启动时间
internal/finish/              扫描落盘目录 + 文件名解析
internal/feishu/              飞书 interactive 卡片 + HMAC 签名 + 去重 + 非阻塞发送
```

更详细的设计与 UML 图见 `kubectl-check-architecture.md` 与 `kubectl-check-uml.html`，完整规格见 `kubectl-check 插件设计规格说明书SPEC.md`。
