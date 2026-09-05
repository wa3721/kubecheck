# kubectl-check 插件设计规格说明书（SPEC）

# 1. 插件概述

## 1.1 设计背景

原生 `kubectl wait` 命令仅校验 Kubernetes 资源的声明式状态（如 Deployment 的 Available 条件），存在核心短板：若 Deployment 未配置就绪探针、存活探针，资源状态就绪后，Pod 仍可能发生启动崩溃、运行异常退出、业务初始化失败等问题。此时原生命令会直接判定部署成功，无法捕获后置异常，且不支持部署后的日志监控与错误告警，导致部署结果判定不准确。

基于该问题，设计 `kubectl-check` 自定义 kubectl 插件，完全兼容原生 kubectl 命令使用风格，用于 `kubectl apply -f` 部署后对 Pod 进行**目标定位 + 就绪校验 + 实时日志监控（LLM 仲裁）+ 异常拦截**，弥补原生命令的校验盲区。

## 1.2 核心能力

- **仅校验 Pod 层级（不校验 Deployment 超时与状态）**：启动时定位本次发布（最新 ReplicaSet）的目标 Pod（含 Pending/拉镜像中），直接进入 Pod 校验；发布刚触发时新 RS 的 Pod 可能尚未创建（1-3 秒竞态窗口），在 `--pod-ready-timeout` 窗口内轮询等待目标 Pod 出现
- **Pod 就绪校验**：逐 Pod 独立超时等待 Ready 条件（含就绪探针校验；无探针容器 Running 即就绪），替代原阶段1 隐含的收敛校验
- **实时日志监控 + LLM 错误仲裁**：每 Pod 独立观察窗口内 Follow 流式采集日志；日志内容判定默认关闭，`--keyword-check` 开启关键字命中、`--llm-enable` 开启仲裁（OpenAI 兼容协议，默认端点智谱 GLM），命中关键字的行由 LLM 异步批量判定真伪，存在真错误才提醒（`--log-dump` 开启时落盘），降低关键字误报
- **全参数可配置**：命名空间、资源名称、两级独立超时、监控规则、错误过滤、LLM 端点等参数支持自定义
- **标准化输出**：区分正常通过、未找到目标 Pod、Pod 异常、日志告警等结果，输出清晰日志与退出码（0/1/2/3/130）
- **错误日志落盘（默认关闭，`--log-dump` 开启）**：开启后 LLM 启用时以容器启动时刻为起点全量拉取覆盖写快照；禁用时回溯 + 增量追加（错误行红色标注）；未发现真错误且未报错退出的正常应用不产生日志文件；关闭时检测/告警/退出码不受影响，仅不写文件
- **飞书告警**：容器异常退出、Pod 状态异常、重启超限、未找到目标 Pod、日志需检查、检查被中断等事件，支持推送飞书机器人

## 1.3 适用场景

- K8s 资源 apply 部署后的最终可用性校验，替代原生 kubectl wait
- 无探针配置的微服务、临时任务、测试服务的部署校验
- CI/CD 流水线自动化部署卡点，避免虚假部署成功
- 部署后短时间内业务日志异常、Pod 重启崩溃的快速排查与拦截
- 常驻监听模式（`-A` 全部命名空间 / `-n` 过滤）：匹配命名空间的 Deployment 创建或更新自动触发检查

# 2. 插件基础信息

|项目|详情|
|---|---|
|插件名称|kubectl-check|
|插件类型|kubectl 自定义插件（遵循 K8s 插件规范，可通过 kubectl 直接调用）|
|调用方式|kubectl check deployment/资源名 [参数]（当前仅支持 deployment 类型）；kubectl check -A 或 kubectl check -n <命名空间过滤> 常驻监听模式|
|兼容版本|Kubernetes 1.20+ 全版本|
|依赖环境|kubectl 客户端、K8s 集群访问权限、容器日志读取权限；（可选）LLM 端点网络可达|

# 3. 核心功能设计 & 技术实现架构

## 3.0 核心技术栈与实现规范

本插件采用 Kubernetes 官方标准技术栈（client-go + cobra）开发：**目标定位为单次 List 查询，Pod 就绪/退出检测采用轻量 1s 轮询**（轮询远比 informer 事件组合简单、可测）；**日志采集采用 `client.CoreV1().Pods().GetLogs()` 流式长连接（Follow）**；**LLM 仲裁客户端以标准库 net/http + encoding/json 实现 OpenAI 兼容协议，无额外依赖**。

### 3.0.1 技术栈选型

- **核心依赖**：**client-go**（Deployment/RS/Pod 均采用定时 List/Get 轮询）
- **命令行框架**：**cobra**（github.com/spf13/cobra），遵循 kubectl 原生参数风格
- **开发语言**：Golang（与 kubectl、client-go 生态统一）
- **实现范式**：目标定位在 `--pod-ready-timeout` 窗口内 1s 轮询（最新 RS revision 的 selector 选 Pod，等待目标 Pod 出现，不校验 Deployment 滚动状态）；阶段2 就绪等待 1s 轮询；阶段3 日志 Follow 流式长连接 + LLM 异步仲裁
- **LLM 客户端**：标准库实现 OpenAI 兼容 chat completions（默认智谱 GLM），非流式、temperature=0、批量判定

### 3.0.2 选型核心优势

- **性能**：目标定位轮询频率低（1s，目标 Pod 出现即停止）；就绪/退出检测轮询频率低（1s），单目标规模下 QPS 开销可控；LLM 异步消费不阻塞日志流，攒批 + 去重控制调用量
- **实时性**：目标 Pod 出现即定位（含 Pending），慢启动应用早期日志不丢失；日志逐行实时采集
- **稳定性**：轮询天然抗集群事件波动；LLM 失败降级、连续失败熔断，不阻塞主流程
- **标准合规**：完全遵循 K8s 客户端开发规范，可无缝兼容所有 K8s 版本环境

## 3.1 检查机制（核心差异化能力）

### 3.1.1 目标定位（locateTargetPods，等待目标 Pod 出现，仅校验 Pod 层级）

启动时定位本次发布（new_rs，最大 revision 的 ReplicaSet）的目标 Pod，直接进入 Pod 层级校验——**不校验 Deployment 的超时与滚动状态**（历史实现 `lockTargetPods` 三分支轮询 / `rolloutComplete` 收敛判定已随"仅校验 Pod 层级"重构删除，`--deploy-ready-timeout` 参数已移除）：

- `spec.replicas == 0`：无目标可等（controller 不会创建任何 Pod），立即发送 `EventNoTargetPod` 告警并以退出码 2 直接退出，不再进入后续阶段
- `spec.replicas > 0`：先单次查询，未命中则在 `--pod-ready-timeout` 窗口内 1s 轮询等待目标 Pod 出现（覆盖"发布刚触发的竞态窗口"——`rollout restart` 后立即启动工具时，新 RS 已被创建但 Pod 尚未创建，通常滞后 1-3 秒）；窗口内未出现 → 同样告警退出（code=2）
- 目标 Pod 出现即定位成功（Pending/拉镜像中即算，无需等待 Ready）；滚动 surge 期间的临时副本也可能被纳入，由阶段2/3 的 DeletionTimestamp 防护兜底（controller 删除中的 Pod 不计为异常）
- `--check-pod-status=false` 时目标定位完成即通过（code=0）

### 3.1.2 阶段2：Pod 就绪校验（waitFirstRunning + waitReady）

目标 Pod 各自独立 goroutine，在 `--pod-ready-timeout`（默认 300s）内以 1s 轮询：

- **前半 waitFirstRunning**：等待 Pod 首次进入 Running（Pending/Init 阶段占用同一预算）；记录 `runningAt` 作为阶段3 日志观察窗口的计时锚点
- **后半 waitReady**：等待就绪——判定条件 `Status.Conditions` 中 `Type == PodReady && Status == ConditionTrue`（等价 `podutil.IsPodReady`，本地实现）；无就绪探针的容器 Running 即 Ready
- 超时未 Running/未就绪 → 置 Pod 失败 + `EventPodStatus` 告警（退出码 3），并联动取消阶段3（该故障仅此一条告警）
- **防误报**：Pod 被 controller 删除（NotFound 或 DeletionTimestamp 非空，surge 缩容/回滚）→ 静默跳过，不计为异常

### 3.1.3 阶段3：日志监控与 LLM 错误仲裁（watchPodLog + recorder）

**由 trackPod 在 Pod 首次进入 Running 时启动（与阶段2 的 Ready 等待并行）**：有就绪探针的 Pod 在 `0/1 Running`（Running 但未 Ready）期间两阶段共同作用；任一路径终态经 podCtx 联动取消另一路。每 Pod 独立观察窗口（`--log-check-timeout`，从 Running 时刻起算，默认 60s）内，两路并行：

- **日志流（rec.TrackLog）**：Follow 流式采集；命中错误关键字（且未被忽略关键字抵消）的行进入仲裁流程（见 3.2）
- **退出检测**：每 1s 检查容器状态——任一容器 `Terminated` 且 `exitCode != 0` → 判定容器异常退出（`EventContainerExit`，退出码 3）并联动取消阶段2（**Ready 前崩溃立即精确告警，无需等就绪超时**）；`--max-restart > 0` 且重启次数超阈值 → `EventRestartLimit`（退出码 3）；controller 删除中的 Pod（DeletionTimestamp 非空）豁免
- 窗口结束无容器崩溃 → 正常结束（存在真错误仅置 warning 提醒，退出码 0）

## 3.2 LLM 日志错误仲裁（`--llm-enable` 开启，默认关闭）

- **判定流程**：命中错误关键字的日志行投入 pod 级有界 channel（容量 256，满则丢弃并计数）；仲裁 goroutine 攒批（10 行 / 2 秒）+ 相同行去重后，以 OpenAI 兼容协议调用 LLM 批量判定；system prompt 要求逐行返回 `{"results":[{"is_error":true|false},...]}`（单批最多 20 行，超出拆分）
- **真错误动作**：任一真错误 → 置 `errorHit` 并发送场景3 提醒；`--log-dump` 开启时以容器启动时刻为起点（`SinceTime`：`State.Running.StartedAt`，已退出取 `LastTerminationState.Terminated.StartedAt`，均不可得回退 Pod 创建时间）从 K8s API **全量拉取**该 Pod 各容器日志，覆盖写快照落盘（首容器 O_TRUNC、后续容器追加）；后续新真错误刷新快照
- **假错误**：全部为假错误 → 不落盘、不告警
- **降级**：LLM 调用失败（超时/网络/解析失败）→ 该批按「关键字即真」处理（宁可误报不漏报）
- **熔断**：连续 3 次调用失败后熔断，后续命中行直接按关键字即真处理，不再发起网络调用
- **禁用**：`--llm-enable=false`（或 `--llm-endpoint=""`）→ 完整回退历史行为（`--log-tail` 回溯 + follow 增量；`--log-dump` 开启时首次命中落盘后增量追加）
- **默认参数**：endpoint/model/api-key 内置智谱 GLM 默认值（`GLM-4-Flash-250414`）；`--llm-enable` 开启后命令行不传即用内置默认值（默认关闭时不发起任何 LLM 调用）
- **不经仲裁的路径**：容器异常退出（退出码 != 0）`--log-dump` 开启时无条件落盘保留现场（LLM 启用时同样 SinceTime 全量快照），并单独告警
- **生命周期**：全部容器日志流结束后 close(channel)，仲裁 goroutine 收尾 flush（上限 40 行，超出丢弃）；观察窗口收尾等待在途判定完成（上限 2×llm-timeout + 快照余量）

## 3.3 超时控制（两段独立，无整体超时）

- `--pod-ready-timeout`（默认 300s）：目标定位等待窗口（等待目标 Pod 出现）+ 阶段2 Pod 就绪等待上限（两段各自独立计时：定位从启动开始，就绪从定位完成后每个 Pod 各自开始）
- `--log-check-timeout`（默认 60s）：阶段3 每 Pod 日志观察窗口，`<=0` 禁用

两段超时相互独立，**无顶层整体超时**；未找到目标 Pod（`replicas=0` 立即 / 窗口内未出现）按退出码 2 退出，Pod 级异常按退出码 3 退出。时间类参数统一为**秒级（int 秒）**。

## 3.4 错误日志落盘（`--log-dump`，默认关闭）

- **默认关闭**（`--log-dump=false`）：日志检测、LLM 仲裁、告警与退出码均不受影响，仅不产生日志文件、告警不附带落盘路径；开启后按以下规则写文件
- 文件命名：`{namespace}-{podName}-{date}.log`（date 格式 20060102，跨天自动切换新文件），目录由 `--log-error-dir` 指定（默认当前工作目录，不存在自动创建）
- **LLM 启用（默认）**：确认真错误后以 `SinceTime` 全量拉取各容器日志，覆盖写整份快照（同 Pod 新真错误重新覆盖，快照始终为最新最全）；错误行 ANSI 红色标注，忽略关键字命中的行不标红
- **LLM 禁用**：Follow 流带 `--log-tail` 行回溯；未命中错误前内存缓冲（环形缓冲），首次命中把「回溯行 + 命中行」落盘（`O_CREATE|O_APPEND|O_WRONLY`），此后逐行增量追加（同日同 Pod 追加同一文件）
- 落盘条件（开启 `--log-dump` 后）：存在真错误（LLM 确认或降级命中），或容器异常退出（保留现场，即便日志未命中关键字）；正常应用不产生日志文件

## 3.5 飞书告警

- 启用方式：`--feishu-webhook` 非空即启用；`--feishu-secret` 开启签名校验（HMAC-SHA256，timestamp+"\n"+secret）
- **未配置 Webhook 降级控制台输出**：所有本应推送飞书的告警事件均降级为控制台直接打印（`[ALERT][事件类型] 标题 + 详情`），同样受去重窗口约束
- 消息格式：飞书自定义机器人 interactive 消息卡片，header 按事件类型着色（red 异常 / orange 提示），elements 用 lark_md 输出资源标识、事件详情、错误日志文件路径、时间戳

| 事件类型 | 触发时机 | 阻塞性 |
|---|---|---|
| 容器异常退出 | 容器 Terminated 且 exit code != 0（`--log-dump` 开启时含日志落盘路径） | 阻塞（退出码 3） |
| 重启次数超限 | 容器 restartCount 超 `--max-restart` | 阻塞（退出码 3） |
| Pod 状态异常 | Pod 在 `--pod-ready-timeout` 内未就绪（Ready） | 阻塞（退出码 3） |
| 未找到目标Pod | 目标定位未找到目标 Pod（`replicas=0` 无目标可等，或窗口内目标 Pod 未出现） | 阻塞（退出码 2） |
| 日志需检查（提示） | 日志窗口结束，Pod 存在真错误日志但存活 | 非阻塞（不影响退出码） |
| 检查被中断 | 收到 SIGINT/SIGTERM | 阻塞（退出码 130） |

- 发送独立 goroutine + 短超时（3s），失败仅记录 warning，不影响主流程判定与退出码；同 key + 同事件在去重窗口（默认 30s）内仅发送一条

# 4. 命令行参数规范（全参数可配置）

## 4.1 基础资源参数

|参数（短/长）|作用|默认值|示例|
|---|---|---|---|
|-n / --namespace|命名空间。单次检查模式（带资源参数）：精确单值；监听模式（无资源参数）：逗号分隔多值 + `*`/`?` 通配（如 `*-prod`）|空（单次模式回填 default）|-n delta / -n '\*-prod' / -n delta,gamma|
|-A / --all-namespaces|监听所有命名空间的 Deployment，创建或更新自动检查（常驻模式）|false|-A|
|资源标识（位置参数）|指定监控资源，格式：类型/名称（当前仅支持 deployment）；监听模式（-A/-n 过滤）下不传|单次检查模式必传|deployment/test-app|

## 4.2 超时与监控时长参数

> 时间类参数统一为**秒级（整数秒）**。

|参数（短/长）|作用|默认值|示例|
|---|---|---|---|
|--pod-ready-timeout|阶段2：Pod 就绪（Ready）超时（秒级）|300|--pod-ready-timeout=60|
|--log-check-timeout|阶段3：每 Pod 日志观察窗口时长（秒级），`<=0` 禁用|60|--log-check-timeout=30|

## 4.3 Pod 异常校验参数

|参数（短/长）|作用|默认值|示例|
|---|---|---|---|
|--max-restart|监控窗口内 Pod 最大允许重启次数，`>0` 启用重启次数检查，`0` 表示禁用该检查|0（禁用）|--max-restart=2|
|--check-pod-status|是否开启 Pod 状态强校验（`false` 时目标定位完成即通过）|true|--check-pod-status=false|

## 4.4 日志监控参数

|参数（短/长）|作用|默认值|示例|
|---|---|---|---|
|--log-enable|是否开启实时日志监控功能|true|--log-enable=false|
|--log-err-keywords|自定义日志异常匹配关键字，多个逗号分隔，仅 --keyword-check 开启时生效|error,panic,fatal,exception,crash|--log-err-keywords=超时,失败|
|--keyword-check|错误关键字判定开关（默认 false 关闭：errorHit 恒 false，场景3 提醒不触发；关键字为 LLM 仲裁唯一入口）；开启后由 --log-err-keywords 决定关键字|false|--keyword-check|
|--log-ignore-keywords|自定义忽略的无害日志关键字，避免误报|空|--log-ignore-keywords=debug,心跳|
|--log-tail|日志流开流时的回溯行数（LLM 禁用路径使用）|100|--log-tail=200|
|--log-error-dir|错误日志落盘目录（文件：{namespace}-{podName}-{date}.log），仅 --log-dump 开启时生效|.（当前目录）|--log-error-dir=./errlogs|
|--log-dump|错误日志落盘开关：默认 false 不落盘（检测/告警不受影响）；开启后命中真错误/容器异常退出时写文件|false|--log-dump|
|--log-console|日志控制台输出开关：默认 true 把追踪阶段读到的日志逐行实时输出到控制台（效果同 kubectl logs --follow，与判定/落盘互不影响）|true|--log-console=false|

## 4.5 LLM 仲裁参数

|参数（短/长）|作用|默认值|示例|
|---|---|---|---|
|--llm-endpoint|日志错误仲裁 LLM 端点（OpenAI 兼容），仅 --llm-enable 开启时生效|智谱 GLM 地址|--llm-endpoint=""|
|--llm-enable|LLM 日志仲裁开关（默认 false 关闭，回退关键字即真）；开启后由 --llm-endpoint 决定端点（需配合 --keyword-check）|false|--llm-enable|
|--llm-model|仲裁模型|GLM-4-Flash-250414|--llm-model=my-model|
|--llm-api-key|仲裁 API Key（内置默认值；仓库公开时注意轮换）|内置|--llm-api-key=xxx|
|--llm-timeout|LLM 单次判定超时（秒级），启用仲裁时必须 > 0|15|--llm-timeout=30|

## 4.6 通用参数

|参数（短/长）|作用|默认值|示例|
|---|---|---|---|
|--feishu-webhook|飞书机器人 Webhook 地址，非空启用告警|空|--feishu-webhook=https://open.feishu.cn/open-apis/bot/v2/hook/xxx|
|--feishu-secret|飞书签名校验密钥（可选，HMAC-SHA256）|空|--feishu-secret=xxx|
|--feishu-dedup-window|同 key + 同事件告警去重窗口（秒级）|30|--feishu-dedup-window=60|
|-v / --verbose|开启详细日志输出（[TRACE] 分阶段步骤日志）|false|-v|
|-h / --help|展示插件帮助文档、参数说明|-|kubectl check -h|

# 5. 执行流程设计

插件采用 **cobra 参数解析 → 目标定位（等待目标 Pod 出现）→ 阶段2 逐 Pod 就绪等待 → 阶段3 日志流式监听 + LLM 仲裁 + 退出检测 → 结果聚合退出** 的标准化流程。

## 5.1 完整执行步骤

1. **参数初始化与校验**：基于 cobra 解析参数，校验资源格式、超时取值范围（`--pod-ready-timeout > 0`、`--log-check-timeout >= 0`、`--max-restart >= 0`、启用仲裁时 `--llm-timeout > 0`）；参数异常返回错误并退出（code=1）。同时构建 K8s 客户端与 LLM 仲裁客户端。

2. **目标定位**：`replicas=0` 立即判定无目标；`replicas>0` 时在 `--pod-ready-timeout` 窗口内 1s 轮询等待本次发布（最新 RS）的目标 Pod 出现（见 3.1.1）；未找到目标 Pod → `EventNoTargetPod` 告警 + 退出码 2（不进入后续阶段）。`--check-pod-status=false` 时目标定位完成即通过（code=0）。

3. **阶段2+3：逐 Pod 并行追踪**：`watchPods` 为每个目标 Pod 启动 goroutine：`waitFirstRunning` 等待首次 Running（1s 轮询，`--pod-ready-timeout` 每 Pod 独立计时）→ 立即并行启动「`waitReady` 就绪等待 + `watchPodLog` 日志观察（`--log-enable` 且 `--log-check-timeout > 0` 时）」；任一路径终态经 podCtx 联动取消另一路；阶段2 超时 → Pod 失败 + `EventPodStatus` 告警；controller 删除中的 Pod 静默跳过。

4. **阶段3：日志观察（两路并行）**：
   - 日志流：`rec.TrackLog` 以 Follow 流式读取；未启用仲裁时命中关键字即标记 errorHit（`--log-dump` 开启时落盘）；启用仲裁时命中行进入 channel 异步判定，存在真错误才置 errorHit（`--log-dump` 开启时 SinceTime 全量落盘）；容器已退出时降级非 Follow 拉历史日志
   - 退出检测：每 1s 检查容器状态，任一容器 `Terminated` 且 `exitCode != 0` → `EventContainerExit`；`--max-restart > 0` 且重启次数超限 → `EventRestartLimit`；删除中的 Pod 豁免
   - 触发退出/超限：停止日志流、等待 flush（LLM 启用时上限 2×llm-timeout+15s，否则 5s）后置 Pod 失败并发送对应告警
   - 日志窗口结束：若 errorHit → `setWarning`（聚合时补发 `EventPendingCheck`）

5. **结果聚合与退出**：等待全部 Pod 追踪结束，任一 Pod 失败 → 退出码 3（异常告警已在各失败路径即时发送：未就绪 `EventPodStatus` / 容器退出 `EventContainerExit` / 重启超限 `EventRestartLimit`，聚合处不重复发送汇总告警）；存在日志告警（warning）→ `EventPendingCheck` 提示告警 + 退出码 0；否则输出"全部 Pod 运行正常" + 退出码 0。

6. **中断处理**：SIGINT/SIGTERM → `Interrupt()` 取消全部上下文 → 等待 goroutine 收尾 → 发送 `EventInterrupted` 告警 → 退出码 130。

## 5.2 异常终止优先级

多异常同时触发时，按以下优先级终止并输出：

**未找到目标 Pod（目标定位，直接退出码 2，不进入下一级）> Pod 异常（容器退出 / 重启超限 / 未就绪，退出码 3）> 日志告警（非阻塞，退出码 0）**

- **日志真错误命中不终止、不即时告警**：仅置 errorHit 并继续追踪（`--log-dump` 开启时全量落盘）；容器随后异常退出/重启超限时触发 `EventContainerExit`/`EventRestartLimit`（退出码 3）；日志存在真错误但 Pod 存活时，窗口结束后补发「需要检查」提示（退出码 0）
- 最终退出码取优先级最高者：任一 Pod 失败 → 3；否则存在日志告警 → 0（提示）；否则 0（正常）

# 6. 输出规范与退出码

## 6.1 日志输出格式

- 正常日志：`[INFO] 监控阶段 + 进度信息`（如 `Deployment <ns>/<name> 全部 Pod 运行正常`）
- 异常日志：`[ERROR] 异常类型 + 详细原因 + 关联 Pod 名称`
- 告警日志：`[ALERT][事件类型] 标题 + 详情`（飞书未配置时控制台降级输出）
- 降级/熔断告警：`[WARN] LLM 日志仲裁调用失败，降级为关键字即真: ...` / `[WARN] LLM 日志仲裁连续 3 次调用失败，熔断...`
- 步骤日志（仅 `-v`）：`[TRACE HH:MM:SS] 阶段N: 步骤详情`（目标定位、Pod phase/ready、日志窗口等）

## 6.2 退出码定义

|退出码|含义|
|---|---|
|0|全部校验通过（如存在"真错误日志但 Pod 存活"，仅补发 `EventPendingCheck` 提示告警，不影响退出码）|
|1|参数解析错误、K8s 客户端构建失败、目标查询失败（cobra RunE 返回错误）|
|2|目标定位未找到目标 Pod（`replicas=0` 无目标可等，或 `--pod-ready-timeout` 窗口内目标 Pod 未出现）|
|3|Pod 异常：Pod 未就绪（Ready 超时）/ 容器异常退出 / 重启次数超限|
|130|收到中断信号（Ctrl+C / SIGTERM）主动退出（发送 `EventInterrupted` 告警，不误报为业务错误）|

# 7. 使用示例

## 7.1 基础用法（默认参数：日志内容判定关闭，仅就绪/退出/重启超限检测）

`kubectl check -n delta deployment/${app}`

功能：目标定位（等待目标 Pod 出现，上限 120s）、Pod 就绪等待（120s）、日志观察 60s，命中错误关键字经 LLM 仲裁，真错误才告警（`--log-dump` 开启时落盘）。

## 7.2 自定义超时与重启阈值

`kubectl check -n delta deployment/${app} --pod-ready-timeout=90 --log-check-timeout=120 --max-restart=1`

## 7.3 禁用 LLM 仲裁（默认即禁用，回退关键字即真）

`kubectl check -n delta deployment/${app} --keyword-check --llm-enable=false`

## 7.3.1 启用关键字命中与 LLM 仲裁

```bash
# 仅关键字（命中即真，无 LLM 调用）
kubectl check -n delta deployment/${app} --keyword-check

# 关键字 + LLM 仲裁（默认端点智谱 GLM）
kubectl check -n delta deployment/${app} --keyword-check --llm-enable
```

## 7.3.1 关闭错误关键字命中（日志内容判定全关，退出检测不受影响）

`kubectl check -n delta deployment/${app} --keyword-check=false`

## 7.4 指向自建 OpenAI 兼容端点（敏感日志不出内网）

`kubectl check -n delta deployment/${app} --llm-endpoint=http://llm.internal:8000/v1/chat/completions --llm-model=my-model --llm-api-key=xxx`

## 7.5 自定义日志关键字与飞书告警

`kubectl check -n delta deployment/${app} --log-err-keywords=业务异常,初始化失败 --log-ignore-keywords=debug日志 --feishu-webhook=https://open.feishu.cn/open-apis/bot/v2/hook/xxx --log-dump --log-error-dir=./errlogs`

## 7.6 常驻监听模式

```bash
kubectl check -A              # 监听全部命名空间
kubectl check -n '*-prod'     # 监听 prod 后缀命名空间（glob 通配）
kubectl check -n delta,gamma  # 监听指定多个命名空间
```

监听模式下匹配命名空间的 Deployment **创建或更新**（generation 递增）自动触发检查（复用单次检查全部逻辑）；启动时已存在的存量 Deployment 不触发（存量抑制）；删除后同名重建（generation 重置）仍触发；SIGINT 退出（码 130）。

# 8. 核心优势总结

- **解决原生痛点**：突破 kubectl wait 仅校验资源状态的局限，适配无探针场景，拦截就绪后 Pod 崩溃异常
- **早期介入**：目标锁定替代收敛等待，Pod 创建齐即锁定，慢启动应用早期日志不丢失；就绪探针校验由阶段2 显式承担
- **LLM 智能仲裁**：真错误才落盘告警，显著降低关键字误报；失败降级、连续失败熔断，无 LLM 环境可显式禁用回退
- **全参数可配置**：状态校验、三段超时、日志规则、异常阈值、LLM 端点均可自定义
- **极简使用**：兼容原生 kubectl 命令风格，学习成本低
- **CI/CD 友好**：标准化退出码（0/1/2/3/130）、清晰异常输出，适配自动化流水线部署卡点

# 9. 关键技术实现细节

## 9.1 轮询与目标发现规范

- **阶段1 轮询**：1s 间隔复合谓词（Get deployment + List RS + List pods），`spec.replicas` 每 tick 重读以跟随 scale；等待上下文不登记中断取消（中断由信号处理流程直接退出，与历史行为一致）
- **Pod 发现**：`listTargetPods` 通过 RS ownerReferences 过滤取最大 revision 的 new_rs，用其 selector（含 pod-template-hash）查询目标 Pod，过滤 Terminating；阶段1 分支A 不使用 deployment selector 兜底（避免误锁旧 Pod）
- **阶段2/3 轮询**：1s 间隔 Get Pod（就绪判定、退出检测），状态查询即时反映最新 API 结果

## 9.2 LLM 仲裁实现细节

- **协议**：OpenAI 兼容 chat completions（智谱 v4 完全兼容）；非流式（不带 stream，解析 `choices[0].message.content`）；temperature=0（判定确定性优先）
- **响应解析**：容忍 ```json 围栏与前后杂散文本（截取首个 `{` 到最后一个 `}`）；结果数量与输入行数强校验，不一致视为失败降级
- **批量拆分**：单批最多 20 行，超出自动拆分为多次调用；相同行去重缓存避免重复判定
- **生命周期**：channel 容量 256（满则丢弃计数，绝不阻塞日志流）；收尾 flush 上限 40 行；窗口收尾等待上限 2×llm-timeout + 10s
- **安全**：API key 仅用于请求头，不落日志；日志行发送至配置端点（默认云端），敏感场景应指向内网端点

## 9.3 与原生 kubectl wait 对齐细节

- 同源处理系统信号（SIGINT/SIGTERM），优雅终止监听、输出退出信息（`Interrupt()` → 取消上下文 → 等待收尾 → 告警 → 退出码 130）
- 同源超时调度模型，基于 context.WithTimeout 实现精准超时管控
- **差异化**：目标锁定 + Pod 就绪 + 日志仲裁三层能力，替代原生命令的单层资源状态校验

## 9.4 扩展能力技术实现

- **防误报**：controller 删除中的 Pod（surge 缩容/回滚）在阶段2（NotFound/DeletionTimestamp）与阶段3（退出检测）均豁免，不计为异常
- **异常快速终止**：容器异常退出、重启超限等即时失败场景，立即取消该 Pod 的日志上下文并置失败，最终聚合后快速退出（码 3）
- **常驻监听（-A / -n 过滤）**：watcher 复用 checker 主流程，Deployment 创建或 generation 递增自动触发检查（存量抑制、删重建可触发、ns 过滤支持多值与通配），零额外逻辑
