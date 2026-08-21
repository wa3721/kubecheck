# kubectl-check 插件设计规格说明书（SPEC）

# 1. 插件概述

## 1.1 设计背景

原生 `kubectl wait` 命令仅校验 Kubernetes 资源的声明式状态（如 Deployment 的 Available 条件），存在核心短板：若 Deployment 未配置就绪探针、存活探针，资源状态就绪后，Pod 仍可能发生启动崩溃、运行异常退出、业务初始化失败等问题。此时原生命令会直接判定部署成功，无法捕获后置异常，且不支持部署后的日志监控与错误告警，导致部署结果判定不准确。

基于该问题，设计 `kubectl-check` 自定义 kubectl 插件，完全兼容原生 kubectl 命令使用风格，用于 `kubectl apply -f` 部署后对 Pod 进行**长效状态校验 + 实时日志监控 + 异常拦截**，弥补原生命令的校验盲区。

## 1.2 核心能力

- 兼容原生 kubectl wait 基础能力，支持 Deployment 就绪状态校验

- 无探针场景适配：不依赖容器探针配置，主动监控 Pod 运行稳定性，拦截就绪后 CrashLoopBackOff、退出重启等异常

- 实时日志监控：每 Pod 独立的日志观察窗口期内持续采集 Pod 日志（Follow 流式），精准捕获业务错误、异常堆栈、启动失败信息

- 全参数可配置：命名空间、资源名称、三段独立超时、监控规则、错误过滤等参数支持自定义

- 标准化输出：区分正常通过、就绪超时、Pod 异常、日志告警等结果，输出清晰日志与退出码（0/1/2/3/130）

- 错误日志落盘：日志关键字命中后不终止监控，将该 Pod 从日志流中采集到的日志**增量追加落盘**（错误行红色标注），供复盘完整错误现场

- 飞书告警：容器异常退出、Pod 状态异常、重启超限、就绪超时、日志需检查、检查被中断等事件，支持推送飞书机器人

## 1.3 适用场景

- K8s 资源 apply 部署后的最终可用性校验，替代原生 kubectl wait

- 无探针配置的微服务、临时任务、测试服务的部署校验

- CI/CD 流水线自动化部署卡点，避免虚假部署成功

- 部署后短时间内业务日志异常、Pod 重启崩溃的快速排查与拦截

# 2. 插件基础信息

|项目|详情|
|---|---|
|插件名称|kubectl-check|
|插件类型|kubectl 自定义插件（遵循 K8s 插件规范，可通过 kubectl 直接调用）|
|调用方式|kubectl check deployment/资源名 [参数]（当前仅支持 deployment 类型）|
|兼容版本|Kubernetes 1.20+ 全版本|
|依赖环境|kubectl 客户端、K8s 集群访问权限、容器日志读取权限|

# 3. 核心功能设计 & 技术实现架构

## 3.0 核心技术栈与实现规范

本插件采用 Kubernetes 官方标准技术栈（client-go + cobra）开发：**第一级 Deployment 就绪校验对标 kubectl wait 采用 informer 事件驱动模型**；**第二级 Pod 状态/日志校验采用轻量轮询 + 流式日志长连接**，保证功能完整与实现可控。

### 3.0.1 技术栈选型

- **核心依赖**：**client-go**（informer 机制用于 Deployment 事件订阅；Pod 侧采用定时 List/Get 轮询）

- **命令行框架**：**cobra**（github.com/spf13/cobra），遵循 kubectl 原生参数风格

- **开发语言**：Golang（与 kubectl、client-go 生态统一）

- **第一级实现范式**：Deployment informer 事件订阅（AddEventHandler + WaitForCacheSync + 首次同步后补查一次），低 QPS、实时响应滚动更新事件

- **第二级实现范式**：Pod 状态与退出检测采用 1s 间隔轮询（List/Get），日志采集采用 `client.CoreV1().Pods().GetLogs()` 流式长连接（Follow）

- **日志采集方式**：原生 `GetLogs()` Follow 流式长连接，贴合官方日志采集规范

### 3.0.2 选型核心优势

- **性能**：第一级 informer 基于本地缓存 + 事件增量推送，无频繁 API 请求；第二级轮询频率低（1s），整体 QPS 开销可控

- **实时性**：Deployment 滚动更新事件即时回调；Pod 日志逐行实时采集，无批量延迟

- **稳定性**：informer 自带断连重连、缓存自愈机制；轮询天然抗集群事件波动

- **标准合规**：完全遵循 K8s 客户端开发规范，可无缝兼容所有 K8s 版本环境

### 3.0.3 与原生 kubectl wait 对齐点

- 第一级就绪校验采用同源 informer 事件订阅方式监听 Deployment 资源变更

- 对齐原生命令的信号处理（SIGINT/SIGTERM）、上下文生命周期管理

- 兼容原生命令的参数解析、异常输出、退出码基础规范（0/1/2/3/130）

- **差异化**：就绪判定不使用 kubectl wait 的 Condition 机制，而是实现 Deployment 滚动完成四条件 `rolloutComplete`（见 3.1.1），并在第二级补充 Pod 状态、容器退出、日志监控能力

## 3.1 双层状态校验机制（核心差异化能力）

插件摒弃原生单层资源状态校验，采用 **资源状态校验 + Pod 运行稳定性校验** 双层机制，彻底解决无探针场景校验失效问题。

### 3.1.1 第一层：资源就绪校验（Deployment informer 实现）

构建 `SharedInformerFactory`，订阅目标 Deployment 的 Add/Update 事件；缓存首次同步完成后立即补查一次最新状态；此后由事件回调触发就绪判定。就绪判定采用自研 `rolloutComplete(d)` 四条件：

1. `observedGeneration >= generation`（controller 已观察到本次 spec）
2. `status.replicas == spec.replicas`（副本数对齐）
3. `updatedReplicas == spec.replicas`（新版本副本数对齐）
4. `readyReplicas == spec.replicas`（就绪副本数对齐）

在 `--deploy-ready-timeout`（默认 300s，`<=0` 禁用）窗口内持续等待；超时未就绪 → 发送 `EventDeployTimeout` 告警并以退出码 2 直接退出，不再进入第二级。

### 3.1.2 第二层：Pod 长效稳定性校验（轮询实现）

Deployment 就绪后，先通过 **Deployment → ReplicaSet（最大 revision）→ Pod** 反查定位本次发布的目标 Pod（`listTargetPods`），过滤 Terminating（DeletionTimestamp 非空）的 Pod；若无法判定最新 RS 则退化为 Deployment selector。随后逐 Pod 并行追踪：

- Pod 状态：`waitPodRunning` 以 1s 间隔轮询，等待 Pod 进入 `Phase == Running`；超时（`--pod-ready-timeout`，默认 120s）→ 置 Pod 失败 + `EventPodStatus` 告警

- 容器退出检测：日志观察阶段每 1s 检查容器状态，任一容器 `Terminated` 且 `exitCode != 0` → 判定容器异常退出（`EventContainerExit`，退出码 3）

- 重启次数：`--max-restart > 0` 时启用，任一容器 `restartCount` 超阈值 → 判定重启超限（`EventRestartLimit`，退出码 3）

- 运行持续性：通过日志观察窗口（`--log-check-timeout`）持续监控 Pod 存活期间的日志与退出行为，规避瞬时就绪后崩溃的假象

## 3.2 实时日志监控与异常捕获

在每 Pod 独立的日志观察窗口（`--log-check-timeout`，从 Pod 进入 Running 时刻起算，默认 60s）内，持续实时采集目标 Pod 日志（Follow 流式，`--log-tail` 回溯），支持日志实时落盘、异常关键字匹配、错误行红色标注。

- 自动关联目标 Deployment 对应的最新 ReplicaSet 下的所有 Pod，无需手动指定 Pod 名称

- 支持自定义错误关键字（默认内置 error、panic、fatal、exception、crash 等核心异常关键词）

- 匹配到异常日志后，**不终止监控**：日志全程增量落盘至 `{namespace}-{podName}-{date}.log`（错误行红色标注），并继续追踪后续状态

- 日志类飞书告警的触发规则：
  - 容器随后异常退出（exit code != 0）→ `EventContainerExit`（退出码 3，附错误日志文件路径）
  - 容器重启次数超限 → `EventRestartLimit`（退出码 3，附重启次数）
  - 日志命中但 Pod 始终存活 → 日志观察窗口结束后补发 `EventPendingCheck`「需要检查」提示告警（非阻塞，不影响退出码）

- 支持日志忽略规则（`--log-ignore-keywords`），可过滤已知无害日志、调试日志，避免误拦截（忽略行不标红、不作为错误根因）

## 3.3 超时控制（三段独立，无整体超时）

- `--deploy-ready-timeout`（默认 300s）：第一级 Deployment 就绪等待上限，`<=0` 禁用（不等待就绪，直接进入第二级）

- `--pod-ready-timeout`（默认 120s）：第二级目标 Pod 出现与进入 Running 的上限

- `--log-check-timeout`（默认 60s）：每 Pod 日志观察窗口长度，`<=0` 禁用（不观察日志）

三段超时相互独立，**无顶层整体超时**；任一级超时按各自退出码退出（2/3/3）。时间类参数统一为**秒级（int 秒）**，支持自定义配置。

## 3.4 错误日志落盘（Follow 实时流增量追加写）

- 文件命名：`{namespace}-{podName}-{date}.log`（date 格式 20060102，跨天自动切换新文件），目录由 `--log-error-dir` 指定（默认当前工作目录，不存在自动创建）

- 记录内容：进入 Running 后由 `GetLogs(Container, TailLines: --log-tail, Timestamps: true, Follow: true)` 开启流式读取，**逐行增量追加**写入文件（`O_CREATE|O_APPEND|O_WRONLY`），不区分是否命中关键字；命中错误关键字的行加 ANSI 红色标注，忽略关键字命中的行不标红

- 红色标注：命中 `--log-err-keywords` 的行以 ANSI 红色转义码包裹，其余行保持默认色；每行带 `[时间戳] [container=xxx]` 前缀便于区分多容器

- 增量追加：同 Pod 后续新日志按时间顺序持续追加到同一文件，新旧错误共存；日志流中断（Pod 退出/窗口结束）时 flush 后关闭文件

## 3.5 飞书告警

- 启用方式：`--feishu-webhook` 非空即启用；`--feishu-secret` 开启签名校验（HMAC-SHA256，timestamp+"\n"+secret）

- **未配置 Webhook 降级控制台输出**：当 `--feishu-webhook` 为空（未启用飞书告警）时，所有本应推送飞书的告警事件（容器异常退出 / Pod 状态异常 / 重启次数超限 / 就绪超时 / 需要检查 / 检查被中断）均**降级为控制台直接打印**（格式 `[ALERT][事件类型] 标题 + 详情`），同样受去重窗口约束避免刷屏；不影响退出码与判定逻辑

- 消息格式：飞书自定义机器人 interactive 消息卡片，header 按事件类型着色（red 异常 / orange 提示），elements 用 lark_md 输出资源标识、事件详情、错误日志文件路径、时间戳

- 触发事件与阻塞性：

| 事件类型 | 触发时机 | 阻塞性 |
|---|---|---|
| 容器异常退出 | 容器 Terminated 且 exit code != 0（含日志落盘路径） | 阻塞（退出码 3） |
| 重启次数超限 | 容器 restartCount 超 `--max-restart` | 阻塞（退出码 3） |
| Pod 状态异常 | 未产生任何 Pod / Pod 未进入 Running 超时 | 阻塞（退出码 3） |
| Deployment 就绪超时 | 第一级 rolloutComplete 四条件超时 | 阻塞（退出码 2） |
| 日志需检查（提示） | 日志窗口结束，Pod 有错误日志但未异常退出 | 非阻塞（不影响退出码） |
| 检查被中断 | 收到 SIGINT/SIGTERM | 阻塞（退出码 130） |

- 发送独立 goroutine + 短超时（3s），失败仅记录 warning，不影响主流程判定与退出码；同 key + 同事件在去重窗口（默认 30s）内仅发送一条

# 4. 命令行参数规范（全参数可配置）

插件参数完全兼容 kubectl 原生参数风格，支持短参数 + 长参数，所有参数均支持自定义，无强制必填参数（资源标识除外，智能自适应默认值）。

## 4.1 基础资源参数

|参数（短/长）|作用|默认值|示例|
|---|---|---|---|
|-n / --namespace|指定目标资源所在命名空间|default|-n delta|
|资源标识（位置参数）|指定监控资源，格式：类型/名称（当前仅支持 deployment）|无，用户必传|deployment/test-app|

## 4.2 超时与监控时长参数

> 时间类参数统一为**秒级（整数秒）**，不再支持 s/m/h 单位后缀。

|参数（短/长）|作用|默认值|示例|
|---|---|---|---|
|--deploy-ready-timeout|第一级：Deployment 就绪等待超时（秒级），`<=0` 禁用|300|--deploy-ready-timeout=600|
|--pod-ready-timeout|第二级：Pod 状态就绪超时（秒级，含目标 Pod 出现等待）|120|--pod-ready-timeout=60|
|--log-check-timeout|每 Pod 日志观察窗口时长（秒级），`<=0` 禁用|60|--log-check-timeout=30|

## 4.3 Pod 异常校验参数

|参数（短/长）|作用|默认值|示例|
|---|---|---|---|
|--max-restart|监控窗口内 Pod 最大允许重启次数，`>0` 启用重启次数检查，`0` 表示禁用该检查|0（禁用）|--max-restart=2|
|--check-pod-status|是否开启 Pod 状态强校验（`false` 时第一级就绪即通过）|true|--check-pod-status=false|

## 4.4 日志监控参数

|参数（短/长）|作用|默认值|示例|
|---|---|---|---|
|--log-enable|是否开启实时日志监控功能|true|--log-enable=false|
|--log-err-keywords|自定义日志异常匹配关键字，多个逗号分隔|error,panic,fatal,exception,crash|--log-err-keywords=超时,失败|
|--log-ignore-keywords|自定义忽略的无害日志关键字，避免误报|空|--log-ignore-keywords=debug,心跳|
|--log-tail|启动监控时回溯读取的日志行数|100|--log-tail=200|
|--log-error-dir|错误日志落盘目录（文件：{namespace}-{podName}-{date}.log，错误行红色标注，Follow 流增量追加写）|.（当前目录）|--log-error-dir=./errlogs|

## 4.5 通用参数

|参数（短/长）|作用|默认值|示例|
|---|---|---|---|
|--feishu-webhook|飞书机器人 Webhook 地址，非空启用告警|空|--feishu-webhook=https://open.feishu.cn/open-apis/bot/v2/hook/xxx|
|--feishu-secret|飞书签名校验密钥（可选，HMAC-SHA256）|空|--feishu-secret=xxx|
|--feishu-dedup-window|同 key + 同事件告警去重窗口（秒级）|30|--feishu-dedup-window=60|
|-v / --verbose|开启详细日志输出（[TRACE] 分阶段步骤日志）|false|-v|
|-h / --help|展示插件帮助文档、参数说明|-|kubectl check -h|

# 5. 执行流程设计

插件采用 **cobra 参数解析 → 第一级 Deployment informer 就绪监听 → 目标 Pod 反查 → 第二级 Pod 轮询追踪 + 日志流式监听 → 结果聚合退出** 的标准化流程。

## 5.1 完整执行步骤

1. **参数初始化与校验**：基于 cobra 命令行框架解析用户传入参数，校验资源格式、超时取值范围（`--deploy-ready-timeout >= 0`、`--pod-ready-timeout > 0`、`--log-check-timeout >= 0`、`--max-restart >= 0`），初始化默认配置；参数异常返回错误并退出（code=1）。同时构建 K8s 客户端（`clientcmd.NewNonInteractiveDeferredLoadingClientConfig`）。

2. **第一层：Deployment informer 就绪监听**：构建 `SharedInformerFactory` 订阅目标 Deployment，等待缓存同步后立即补查一次，并持续由 Add/Update 事件触发 `rolloutComplete` 四条件判定；在 `--deploy-ready-timeout` 窗口内未就绪 → `EventDeployTimeout` 告警 + 退出码 2（不进入第二级）。`--check-pod-status=false` 时此处直接通过（code=0）。

3. **关联 Pod 自动发现**：`listTargetPods` 通过 ReplicaSet 反查——列出该 Deployment 控制的 ReplicaSet（按 ownerReferences 过滤），取 revision 最大的作为本次发布 new_rs，用其 selector（含 pod-template-hash）查询目标 Pod，过滤 Terminating Pod；无法判定最新 RS 时退化为 Deployment selector。`waitForTargetPods` 在 `--pod-ready-timeout` 窗口内以 1s 间隔轮询直到发现目标 Pod，超时仍无 Pod → `EventPodStatus` 告警 + 退出码 3。

4. **第二级：逐 Pod 并行追踪**：`watchPods` 为每个目标 Pod 启动 goroutine：先 `waitPodRunning` 等待 `Phase == Running`（1s 轮询，`--pod-ready-timeout` 每 Pod 独立计时），超时 → Pod 失败 + `EventPodStatus` 告警；进入 Running 且 `--log-enable` 时启动日志观察。

5. **日志观察（watchPodLog，两路并行）**：
   - 日志流：`rec.TrackLog` 以 Follow 流式读取并逐行增量落盘（错误行标红），命中错误关键字（且未命中忽略规则）标记 errorHit；
   - 退出检测：每 1s 检查容器状态，任一容器 `Terminated` 且 `exitCode != 0` → `EventContainerExit`；`--max-restart > 0` 且重启次数超限 → `EventRestartLimit`；
   - 触发退出/超限：停止日志流、等待 flush（5s 兜底）后置 Pod 失败并发送对应告警；
   - 日志窗口结束（`--log-check-timeout` 到期）：若 errorHit → `setWarning`（聚合时补发 `EventPendingCheck`）。

6. **结果聚合与退出**：等待全部 Pod 追踪结束，任一 Pod 失败 → `EventContainerExit` 聚合告警 + 退出码 3；存在日志告警（warning）→ `EventPendingCheck` 提示告警 + 退出码 0；否则输出"全部 Pod 运行正常" + 退出码 0。

7. **中断处理**：SIGINT/SIGTERM → `Interrupt()` 取消全部上下文 → 等待 goroutine 收尾 → 发送 `EventInterrupted` 告警 → 退出码 130。

## 5.2 异常终止优先级

多异常同时触发时，按以下优先级终止并输出：

**Deployment 就绪超时（第一级，直接退出码 2，不进入下一级）> Pod 异常（容器退出 / 重启超限 / 未进入 Running / 未产生 Pod，退出码 3）> 日志告警（非阻塞，退出码 0）**

- **日志关键字命中不终止、不即时告警**：仅增量落盘日志并继续追踪；容器随后异常退出/重启超限时触发 `EventContainerExit`/`EventRestartLimit`（退出码 3）；日志命中但 Pod 存活时，窗口结束后补发「需要检查」提示（退出码 0）

- 最终退出码取优先级最高者：任一 Pod 失败 → 3；否则存在日志告警 → 0（提示）；否则 0（正常）

# 6. 输出规范与退出码

## 6.1 日志输出格式

标准输出采用结构化简洁格式，verbose（`-v`）模式下输出分阶段 [TRACE] 步骤日志，便于对照执行时序。

- 正常日志：`[INFO] 监控阶段 + 进度信息`（如 `Deployment <ns>/<name> 全部 Pod 运行正常`）

- 异常日志：`[ERROR] 异常类型 + 详细原因 + 关联 Pod 名称 + 日志片段`

- 告警日志：`[ALERT][事件类型] 标题 + 详情`（飞书未配置时控制台降级输出）

- 步骤日志（仅 `-v`）：`[TRACE HH:MM:SS] 阶段N: 步骤详情`（滚动进度快照、new_rs 定位、Pod phase、日志窗口等）

## 6.2 退出码定义

|退出码|含义|
|---|---|
|0|全部校验通过（如有"日志命中但 Pod 存活"的 Pod，仅补发 `EventPendingCheck` 提示告警，不影响退出码）|
|1|参数解析错误、K8s 客户端构建失败、目标 Pod 查询失败（cobra RunE 返回错误）|
|2|第一级 Deployment 就绪超时（rolloutComplete 四条件未在 `--deploy-ready-timeout` 内满足）|
|3|Pod 异常：未产生任何 Pod / Pod 未进入 Running / 容器异常退出 / 重启次数超限|
|130|收到中断信号（Ctrl+C / SIGTERM）主动退出（发送 `EventInterrupted` 告警，不误报为业务错误）|

# 7. 使用示例

## 7.1 基础用法（默认参数）

`kubectl check -n delta deployment/${app}`

功能：默认等待 Deployment 就绪（300s）、Pod 进入 Running（120s）、观察日志 60s。

## 7.2 自定义超时与重启阈值

`kubectl check -n delta deployment/${app} --deploy-ready-timeout=600 --pod-ready-timeout=90 --log-check-timeout=120 --max-restart=1`

功能：就绪等待 600 秒，Pod 就绪等待 90 秒，日志观察 120 秒，允许 Pod 最多重启 1 次。

## 7.3 自定义日志异常关键字

`kubectl check -n delta deployment/${app} --log-err-keywords=业务异常,初始化失败 --log-ignore-keywords=debug日志`

## 7.4 开启详细日志输出

`kubectl check -n delta deployment/${app} -v`

## 7.5 启用飞书告警与错误日志落盘

`kubectl check -n delta deployment/${app} --feishu-webhook=https://open.feishu.cn/open-apis/bot/v2/hook/xxx --feishu-secret=yyy --log-error-dir=./errlogs`

# 8. 核心优势总结

- **解决原生痛点**：突破 kubectl wait 仅校验资源状态的局限，适配无探针场景，拦截就绪后 Pod 崩溃异常

- **原生技术对齐**：基于 client-go 开发，第一级对标 kubectl wait 的事件驱动模型，官方标准无兼容风险

- **全参数可配置**：状态校验、三段超时、日志规则、异常阈值均可自定义，适配不同业务场景

- **极简使用**：兼容原生 kubectl 命令风格，学习成本低，可直接替换原有 wait 命令

- **CI/CD 友好**：标准化退出码（0/1/2/3/130）、清晰异常输出，适配自动化流水线部署卡点

- **实时问题感知**：部署后即时捕获业务日志错误并增量落盘（错误行红色标注），日志根因失败/未退出待检查均推送飞书，无需人工二次核查 Pod 状态与日志

# 9. 关键技术实现细节

## 9.1 资源监听规范

- **Deployment 侧 informer**：单次插件调用仅初始化一组 Deployment informer（`SharedInformerFactory`），避免重复注册导致的事件重复回调、内存泄漏；绑定第一级超时上下文（`context.WithTimeout`），任务结束/超时/异常时自动 Stop，彻底释放资源

- **Pod 侧轮询**：第二级不使用 Pod informer，采用 1s 间隔 List/Get 轮询（`waitPodRunning`、退出检测、`waitForTargetPods`），状态查询即时反映最新 API 结果

- **缓存容错机制**：第一级复用 client-go 自带本地缓存，支持断连重连、资源快照恢复，保证监控不中断

## 9.2 与原生 kubectl wait 对齐细节

- 第一级使用同源 informer 事件订阅模型（AddEventHandler + WaitForCacheSync），而非手动 List 轮询

- 就绪判定采用自研 `rolloutComplete` 四条件（observedGeneration / status.replicas / updatedReplicas / readyReplicas 与 spec.replicas 对齐），不使用 Condition 机制

- 同源处理系统信号（SIGINT/SIGTERM），优雅终止监听、输出退出信息（`Interrupt()` → 取消上下文 → 等待收尾 → 告警 → 退出码 130）

- 同源超时调度模型，基于 context.WithTimeout 实现精准超时管控

## 9.3 扩展能力技术实现

- **Pod 稳定性扩展**：在 Deployment 就绪事件回调基础上，新增 Pod 状态轮询、容器退出检测、重启计数检查，补充原生缺失的运行态校验

- **日志监控扩展**：Pod 进入 Running 后自动启动 Follow 日志流长连接与退出检测并行执行，互不阻塞；日志逐行增量落盘（错误行标红），命中错误关键字不终止监控

- **异常快速终止**：容器异常退出、重启超限等即时失败场景，立即取消该 Pod 的日志上下文并置失败，最终聚合后快速退出（码 3）；日志关键字命中除外（仅落盘，不终止，继续追踪）
