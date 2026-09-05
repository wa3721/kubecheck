# kubectl-check

`kubectl-check` 是 `kubectl` 插件，用于部署后的自动化校验：**无整体超时控制**，不校验 Deployment 的超时与滚动状态，仅校验 Pod 层级（两级独立超时）：

- **目标定位（仅校验 Pod 层级）**：启动时定位本次发布（最新 ReplicaSet）的目标 Pod，**不校验 Deployment 滚动状态**；发布刚触发时新 RS 的 Pod 可能尚未创建，在 `--pod-ready-timeout` 窗口内等待目标 Pod 出现（Pending 即算定位成功）；`replicas=0` 无目标可等或窗口内未出现 → **发送告警（优先飞书，无飞书降级控制台）后直接退出**（退出码 2）
- **阶段2（就绪等待）**：目标 Pod 各自独立 `--pod-ready-timeout` 等待**就绪（Ready 条件，含就绪探针校验；无探针容器 Running 即就绪）**。优先级：**Pod 未就绪 > 日志报错**。
- **阶段3（日志观察，与阶段2 并行）**：Pod 首次进入 Running 即启动（非等 Ready），独立 `--log-check-timeout` 窗口（从 Running 时刻起算）；有探针时 `0/1 Running` 期间两阶段共同作用，任一终态联动取消另一路（同一故障仅一条告警）。窗口结束无容器崩溃 → 正常退出（命中真错误仅发送提醒，退出码 0）。日志内容判定默认全部关闭（`--keyword-check` / `--llm-enable` 显式开启）：
  - 场景1：Pod 在超时内就绪且无真错误日志 → 正常退出（码 0）。
  - 场景2：Pod 超时未就绪 → 发送告警，程序**异常退出**（码 3）。
  - 场景3：Pod 已就绪但日志存在**真错误** → 发送提醒「xx pod 存在错误日志，但 pod 已就绪，需要检查」（不退出，码 0）；`--log-dump` 开启时以容器启动时刻为起点全量拉取日志**落盘**。
  - 关键字命中（`--keyword-check`）：LLM 仲裁开启时真错误才提醒/落盘，假错误静默；LLM 关闭（默认）时命中即真
  - LLM 调用失败 → 降级为「关键字即真」；连续失败自动熔断不再重试
  - 容器异常退出（退出码 != 0）不经仲裁、不受开关影响（`--log-dump` 开启时无条件落盘保留现场）

基于纯官方生态实现：**cobra** 命令行框架 + **client-go**（LLM 客户端为标准库实现，无额外依赖）。

---

## 特性

- **仅校验 Pod 层级**：不校验 Deployment 滚动状态与收敛、无独立目标锁定参数，启动即定位最新 RS 的目标 Pod（含 Pending，未出现则等待）直接检查；就绪探针校验由阶段2 承担。
- **两段独立超时，无整体超时**：`--pod-ready-timeout`（Pod 就绪）/ `--log-check-timeout`（日志跟踪）彼此独立。
- **就绪探针校验**：阶段2 等待 Pod Ready 条件（等价 `kubectl wait --for=condition=ready`），探针失败会在就绪超时被捕获。
- **LLM 日志错误仲裁（`--llm-enable` 开启，默认关闭）**：命中关键字的日志行经 LLM 批量判定真伪（攒批 + 去重），只有真错误才告警（`--log-dump` 开启时落盘），降低关键字误报；失败降级、连续失败熔断。
- **日志控制台输出（默认开启，`--log-console`）**：追踪流读到的日志行逐行实时输出到控制台（原始行，效果同 `kubectl logs --follow`）；关闭则静默。与关键字/LLM 判定、落盘互不影响。
- **错误日志落盘（默认关闭，`--log-dump` 开启）**：开启后命中真错误/容器异常退出时写 `{namespace}-{podName}-{date}.log`，错误行 ANSI 红色标注；关闭时检测/告警不受影响，仅不写文件。
  - LLM 启用：真错误时以**容器启动时刻（SinceTime）为起点全量拉取**，覆盖写快照（新真错误刷新快照）
  - LLM 禁用：`--log-tail` 行回溯 + follow 增量，首次命中落盘后增量追加
- **防误报**：controller 删除中的 Pod（surge 缩容/回滚）不计为异常；忽略关键字命中的行不参与判定与标红。
- **飞书告警**：优先飞书；**未配置 Webhook 降级控制台**，告警事件不静默丢弃。
- **主动退出**：收到 `SIGINT`/`SIGTERM` 时优雅退出（退出码 130），不误报为业务错误。

---

## 安装与构建

```bash
make tidy      # 整理依赖
make build     # 编译二进制到 ./bin/kubectl-check（默认 linux/本机架构）
make build GOOS=windows   # 交叉编译 windows 二进制（bin/kubectl-check-windows-<arch>.exe）
make build GOOS=darwin    # 交叉编译 macOS 二进制（可加 GOARCH=amd64/arm64）
make test      # 运行全部单元测试
make all       # 交叉编译多平台产物
make install   # 安装到 GOBIN
```

依赖：`github.com/spf13/cobra`、`k8s.io/client-go`（版本见 `go.mod`）。

---

## 使用方式

作为 kubectl 插件使用（二进制命名为 `kubectl-check` 并放入 `PATH`）：

```bash
# 基本用法：检查 delta 命名空间下 deployment/my-app（两级超时独立控制）
kubectl check -n delta deployment/my-app

# 自定义两级独立超时（秒级）
kubectl check -n delta deployment/my-app \
  --pod-ready-timeout=300 --log-check-timeout=60

# 启用错误日志落盘（默认关闭）与飞书告警
kubectl check -n delta deployment/my-app \
  --log-error-dir=./errlogs \
  --feishu-webhook=https://open.feishu.cn/open-apis/bot/v2/hook/xxx \
  --feishu-secret=yyy

# 自定义日志关键字与忽略规则
kubectl check -n delta deployment/my-app \
  --log-err-keywords=error,panic,fatal \
  --log-ignore-keywords="level=debug"

# 启用错误关键字命中（LLM 默认关闭，命中即真）
kubectl check -n delta deployment/my-app --keyword-check

# 启用 LLM 仲裁（需配合 --keyword-check 提供命中输入）
kubectl check -n delta deployment/my-app --keyword-check --llm-enable

# 禁用 LLM 仲裁（默认即禁用；--log-dump 开启时命中即落盘）
kubectl check -n delta deployment/my-app --keyword-check --llm-enable=false

# 关闭错误关键字命中（默认即关闭，日志内容判定全关，退出检测不受影响）
kubectl check -n delta deployment/my-app --keyword-check=false

# 指向自建 OpenAI 兼容端点（敏感日志不出内网；需 --keyword-check --llm-enable 开启）
kubectl check -n delta deployment/my-app --keyword-check --llm-enable \
  --llm-endpoint=http://llm.internal:8000/v1/chat/completions \
  --llm-model=my-model --llm-api-key=xxx
```

> 资源参数支持 `deployment/my-app` 与 `deployment my-app` 两种 kubectl 风格。
> 监听模式（常驻）：`-A/--all-namespaces` 监听所有命名空间；或 `-n` 指定命名空间过滤后**不带资源参数**（支持逗号分隔多值与 `*` / `?` 通配，如 `-n '*-prod'`）。匹配命名空间的 Deployment **创建或更新**（generation 递增）时自动触发检查；启动时已存在的存量 Deployment 不触发。

---

## 参数说明

| 参数 | 作用 | 默认值 |
| --- | --- | --- |
| `-A / --all-namespaces` | 监听所有命名空间，Deployment 创建/更新自动检查（常驻模式，无需资源参数） | `false` |
| `-n / --namespace` | 命名空间。单次检查模式（带资源参数）：精确单值，未指定用 `default`；监听模式（无资源参数）：逗号分隔多值 + `*` / `?` 通配（如 `*-prod`） | 空（单次模式回填 `default`） |
| 位置参数 `类型/名称` | 监控资源（当前仅 `deployment`） | 必传 |
| `--pod-ready-timeout` | 阶段2：Pod 就绪（Ready）超时（秒） | `300` |
| `--log-check-timeout` | 阶段3：日志跟踪超时（秒），`<=0` 禁用 | `60` |
| `--max-restart` | Pod 最大允许重启次数 | `0` |
| `--check-pod-status` | Pod 状态强校验开关 | `true` |
| `--log-enable` | 实时日志监控开关 | `true` |
| `--log-err-keywords` | 日志异常关键字（逗号分隔），仅 `--keyword-check` 开启时生效 | `error,panic,fatal,exception,crash` |
| `--keyword-check` | 错误关键字判定开关（默认 `false` 关闭：`errorHit` 恒 false，场景3 提醒不触发；关键字为 LLM 仲裁唯一入口）；开启后由 `--log-err-keywords` 决定关键字 | `false` |
| `--log-ignore-keywords` | 忽略的无害日志关键字（逗号分隔） | 空 |
| `--log-tail` | 日志流开流时的回溯行数（LLM 禁用路径使用） | `100` |
| `--log-error-dir` | 错误日志落盘目录（文件：`namespace-podname-date.log`），仅 `--log-dump` 开启时生效 | `.` |
| `--log-dump` | 错误日志落盘开关：默认 `false` 不落盘；开启后命中真错误/容器异常退出时写文件 | `false` |
| `--log-console` | 日志控制台输出开关：默认 `true` 把追踪阶段读到的日志逐行实时输出到控制台（效果同 `kubectl logs --follow`，与判定/落盘互不影响） | `true` |
| `--llm-endpoint` | 日志错误仲裁 LLM 端点（OpenAI 兼容），仅 `--llm-enable` 开启时生效 | 智谱 GLM 地址 |
| `--llm-enable` | LLM 日志仲裁开关（默认 `false` 关闭，回退关键字即真）；开启后由 `--llm-endpoint` 决定端点（需配合 `--keyword-check`） | `false` |
| `--llm-model` | 仲裁模型 | `GLM-4-Flash-250414` |
| `--llm-api-key` | 仲裁 API Key | 内置（见下「安全提示」） |
| `--llm-timeout` | LLM 单次判定超时（秒） | `15` |
| `--feishu-webhook` | 飞书机器人 Webhook，非空启用告警 | 空 |
| `--feishu-secret` | 飞书签名密钥（HMAC-SHA256） | 空 |
| `--feishu-dedup-window` | 同 Pod+同事件告警去重窗口（秒） | `30` |
| `-v / --verbose` | 详细日志输出 | `false` |
| `-h / --help` | 帮助文档 | - |

---

## 退出码

| 退出码 | 含义 |
| --- | --- |
| `0` | 正常：场景1（Pod 就绪且无真错误日志）或场景3（Pod 就绪但存在真错误日志，已提醒；`--log-dump` 开启时已落盘） |
| `1` | 参数解析错误、目标资源不存在 |
| `2` | 目标定位未找到目标 Pod（`replicas=0` 无目标可等，或 `--pod-ready-timeout` 窗口内未出现，告警后直接退出，不继续追踪 Pod） |
| `3` | 场景2：Pod 超时未就绪 / 重启超限 / 容器异常退出，异常退出 |
| `130` | 收到中断信号（Ctrl+C / SIGTERM）主动退出 |

---

## LLM 日志错误仲裁

- **判定方式**：命中错误关键字（且未被忽略关键字抵消）的日志行投入 pod 级有界队列，异步攒批（10 行 / 2 秒）+ 相同行去重后，以 OpenAI 兼容协议调用 LLM 批量判定；system prompt 要求逐行返回 `{"results":[{"is_error":true|false},...]}`。
- **真错误动作**：任一真错误 → 发送场景3 提醒；`--log-dump` 开启时以容器启动时刻为起点（`SinceTime`）从 K8s API **全量拉取**该 Pod 各容器日志，覆盖写快照落盘；后续新真错误刷新快照。
- **降级与熔断**：调用失败（超时/网络/解析失败）该批按「关键字即真」处理；连续 3 次失败后熔断，后续命中直接按关键字即真，不再发起网络调用。
- **启用**：`--llm-enable`（默认关闭；开启时建议同时 `--keyword-check`，否则无命中输入）；判定协议 OpenAI 兼容，默认端点智谱 GLM。
- **禁用（默认）**：`--llm-enable=false`（或 `--llm-endpoint=""`）→ 完整回退历史行为（`--log-tail` 回溯 + follow 增量；`--log-dump` 开启时首次命中落盘后追加）。
- **不经仲裁的路径**：容器异常退出（退出码 != 0）`--log-dump` 开启时无条件落盘保留现场（LLM 启用时同样全量快照）。

### 安全提示

- **默认 API Key 内置于源码**（`internal/llm` 常量）：仓库若公开有泄露风险，请用 `--llm-api-key` 覆盖并到智谱控制台轮换内置 key。
- **日志内容会发送至 LLM 端点**（默认为云端）：敏感日志场景请 `--llm-endpoint` 指向内网自建的 OpenAI 兼容端点。
- API Key 仅出现在请求头，不会写入任何日志输出或落盘文件。

---

## 错误日志落盘

- **默认关闭**（`--log-dump=false`）：日志检测、LLM 仲裁、告警与退出码均不受影响，仅不产生日志文件、告警不附带落盘路径；开启 `--log-dump` 后按以下规则写文件。
- 文件命名：`{namespace}-{podName}-{date}.log`（`date` 格式 `20060102`，跨天自动切换），目录由 `--log-error-dir` 指定。
- 记录内容（按 LLM 启用与否两条路径）：
  - **LLM 启用（`--llm-enable`）**：确认真错误后，以容器启动时间为起点（`State.Running.StartedAt`；已退出取 `LastTerminationState.Terminated.StartedAt`；均不可得回退 Pod 创建时间）**全量拉取**从启动到当前的日志，覆盖写整份快照。
  - **LLM 禁用（默认）**：follow 流带 `--log-tail` 行回溯；命中关键字前缓冲在内存环形缓冲，首次命中把「回溯行 + 命中行」落盘，此后增量追加（同日同 Pod 追加同一文件）。
- 错误行用 ANSI 红色转义码标注，忽略规则命中的行不标红。

---

## 项目结构

```
main.go                      入口，调用 cmd.Execute()
internal/cmd/                 cobra 命令、参数解析、客户端构建
internal/options/             Options 参数定义（两级独立超时 + LLM 参数秒级转换）
internal/checker/             核心编排：目标定位 / 阶段2 就绪等待 / 阶段3 日志+仲裁调度 / 场景判定
internal/recorder/            日志采集与落盘：关键字路径 + LLM 仲裁路径（channel 攒批/熔断/SinceTime 全量快照）
internal/llm/                 OpenAI 兼容 LLM 客户端（默认智谱 GLM，标准库实现）
internal/finish/              扫描落盘目录 + 文件名解析
internal/feishu/              飞书 interactive 卡片 + HMAC 签名 + 去重 + 非阻塞发送
internal/watcher/             监听模式（-A 全部 / -n 过滤，Deployment 创建/更新自动触发检查，存量抑制）
```

更详细的设计与 UML 图见 `doc/kubectl-check-architecture.md` 与 `doc/kubectl-check-uml.html`，完整规格见 `doc/kubectl-check 插件设计规格说明书SPEC.md`。
