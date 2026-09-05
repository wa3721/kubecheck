# kubectl-check E2E 测试报告

# 1. 测试概述

## 1.1 测试目的

本报告记录 `kubectl-check` 插件（`kubecheck`，Go + cobra + client-go 实现）在**真实 Kubernetes 集群**上的端到端（E2E）验证结果。E2E 测试不依赖单元测试的 mock 集群，而是通过真实 Deployment 滚动更新、真实 Pod 生命周期、真实日志流式采集，逐用例验证插件三阶段检查链路的端到端行为：

- **阶段1**：目标锁定（三分支轮询：收敛有 Pod 立即锁 / 收敛无 Pod 等更新 / 滚动等名单齐备）
- **阶段2**：Pod 就绪等待（PodReady 条件，含就绪探针校验）
- **阶段3**：实时日志监控 + LLM 错误仲裁（本报告全部用例显式禁用 LLM 验证关键字降级路径；LLM 仲裁逻辑由单测覆盖）+ 结果判定与告警（退出码 0/1/2/3/130）

## 1.2 测试范围

| 项 | 说明 |
|---|---|
| 测试对象 | `kubectl-check`（kubecheck 二进制，`/tmp/flowbin`） |
| 测试脚本 | `e2e/e2e-test.sh`（用法：`bash e2e-test.sh [C1\|...\|C14\|ALL]`，默认 ALL） |
| 测试资源 | `test/flow-c1.yaml` ~ `test/flow-c13.yaml`（C1~C7、C14 由脚本内联创建，C8~C13 由 yaml 文件创建） |
| 测试命名空间 | `flowtest` |
| 日志落盘目录 | `/tmp/flowlogs` |
| LLM 仲裁 | 全部用例 `--llm-endpoint ""` 显式禁用（保证断言确定性、不消耗云端配额） |
| 覆盖用例数 | 14（C1~C14） |
| 断言数 | 19（含 5 个用例的附加落盘断言） |

## 1.3 测试环境

| 项 | 详情 |
|---|---|
| 集群 | Kubernetes v1.36.3（单节点 WSL 环境） |
| 网络插件 | Calico（CNI，containerd 运行时） |
| 客户端 | kubectl + client-go v0.31.0 |
| 运行环境 | WSL Ubuntu-22.04（Windows CMD 经 `wsl -d Ubuntu-22.04` 调用） |
| 测试时间 | 2026-08-22 ~ 2026-09-03 |

---

# 2. 测试结果总览

**最终回归结果：PASS=19 FAIL=0，全部 14 个用例通过，无回归。**

| 用例 | 场景描述 | 期望退出码 | 附加断言 | 结果 |
|---|---|---|---|---|
| C1 | 正常通过（目标锁定+就绪通过、日志无错误） | 0 | - | PASS |
| C2 | 阶段2 Pod 未就绪（readiness 探针失败） | 3 | - | PASS |
| C3 | 阶段2 Pod 未就绪（Pending 未调度） | 3 | - | PASS |
| C4 | 阶段3 容器异常退出（退出码 != 0） | 3 | - | PASS |
| C5 | 阶段3 日志命中错误关键字但 Pod 存活 | 0（warning） | - | PASS |
| C6 | 中断信号（SIGINT） | 130 | - | PASS |
| C7 | -A 监听模式：deployment 更新自动触发检查，中断 watcher | 130 | - | PASS |
| C8 | 日志命中错误关键字但被 ignore 忽略 | 0 | **不落盘** | PASS |
| C9 | 多容器：sidecar 先就绪后报错退出 | 3 | **已落盘** | PASS |
| C10 | 重启次数超限（`--max-restart` 触发） | 3 | - | PASS |
| C11 | 多副本聚合（2 副本均先就绪后报错退出） | 3 | **2 个落盘文件** | PASS |
| C12 | 无错误关键字（仅 INFO/WARN）→ 缓冲后丢弃 | 0 | **不落盘** | PASS |
| C13 | 命中错误 + ignore 混合 → 落盘仅含非 ignore 错误行 | 0（warning） | **落盘内容校验** | PASS |
| C14 | 阶段1 目标锁定超时（已收敛且无 Pod，窗口内无更新） | 2 | - | PASS |

> 说明：PASS 计数为 19 = 14 个用例主断言 + 5 个附加落盘断言（C8/C9/C11/C12/C13）。

---

# 3. 用例详解

## 3.1 C1 正常通过（exit 0）

- **场景**：`nginx:1.25` Deployment 正常滚动更新，Pod Ready 且日志无错误关键字。
- **验证链路**：阶段1 分支B 立即锁定现存 Pod → 阶段2 PodReady 就绪（耗时 0.0 秒）→ 阶段3 日志追踪 10s 无错误命中。
- **判定关键字**：`全部 Pod 运行正常`。

## 3.2 C2 阶段2 Pod 未就绪——探针失败（exit 3）

- **场景**：`busybox` + `readinessProbe: /bin/false`（探针永远失败）→ Pod Running 但 Ready=0/1。
- **验证链路**：阶段1 目标锁定不看收敛（Pod 已创建齐即锁定，阶段1 秒过）→ 阶段2 `waitPodReady` 等待 PodReady 条件超时（`--pod-ready-timeout 15`）→「未就绪（Ready 超时）」告警 + exit 3。
- **判定关键字**：`未就绪`。
- **语义变化**：旧版本该场景为阶段1 收敛超时 exit 2；新逻辑下就绪探针校验由阶段2 显式承担，为 exit 3（预期变化）。

## 3.3 C3 阶段2 Pod 未就绪——Pending 未调度（exit 3）

- **场景**：replicas=0 收敛后启动工具（阶段1 走"已收敛无 Pod → 等更新"分支）；节点施加 `flowtest=block:NoSchedule` taint 后 scale 出 2 个 Pending Pod。
- **验证链路**：scale 递增 generation → 阶段1 锁定 2 个 Pending Pod（名单齐备即锁定，无需调度）→ 阶段2 就绪超时 → 告警「未就绪」+ exit 3。
- **判定关键字**：`未就绪`。

## 3.4 C4 阶段3 容器异常退出（exit 3）

- **场景**：容器 `sleep 45; exit 1` → Pod 先就绪，45s 后容器以退出码 1 终止。
- **验证链路**：阶段3 捕获 `ContainerTerminated` 且 exit code != 0 → 判定容器异常退出，告警后异常退出。
- **判定关键字**：`容器异常退出`。

## 3.5 C5 阶段3 日志报错但 Pod 存活（exit 0，warning）

- **场景**：容器循环输出 `ERROR something went wrong`，Pod 存活不退出；LLM 显式禁用。
- **验证链路**：阶段3 命中错误关键字 `ERROR`（禁用 LLM 时关键字即真）→ 落盘 + 置 warning；Pod 未退出，最终以 0 退出并提示「日志需检查」。
- **判定关键字**：`日志需检查`。

## 3.6 C6 中断信号（exit 130）

- **场景**：正常 Deployment，工具运行 5s 后向其发送 SIGINT。
- **验证链路**：信号处理链路 → 优雅退出，退出码 130，不误报为业务错误。
- **判定关键字**：`检查被中断`。

## 3.7 C7 -A 监听模式（exit 130）

- **场景**：`kubectl check -A` 后台启动 watcher → `rollout restart` 触发 Deployment generation 递增 → watcher 自动启动检查 → SIGINT 中断 watcher。
- **验证链路**：-A 全命名空间监听 → informer 识别 generation 变化 → 自动触发检查（日志命中「检测到 Deployment ... 更新」与「... 检查完成」）→ 中断退出 130。watcher 触发的检查自动走新阶段1（触发时未收敛 → 立即盯新 RS Pod，比旧逻辑更早）。
- **判定关键字**：`检测到 Deployment`。

## 3.8 C8 忽略关键字优先（exit 0，不落盘）

- **场景**：容器输出 `[ERROR] this is an expected error`，同时指定 `--log-err-keywords ERROR --log-ignore-keywords expected`。
- **验证链路**：recorder 命中判定 `hitAny(line, errKw) && !hitIgnore(line, ignoreKw)` —— ignore 优先级高于 error，该行不标红、不算 errorHit、不落盘。
- **附加断言**：`/tmp/flowlogs` 下**不产生** `flowtest-flow-c8-*.log` 文件。

## 3.9 C9 多容器（exit 3，落盘）

- **场景**：Pod 含 `main`（持续存活）+ `sidecar`（先 `sleep 30` 保证 Pod Ready，后输出 ERROR 并以 exit 3 退出）两个容器。
- **验证链路**：recorder 遍历 `pod.Status.ContainerStatuses`，捕获 sidecar 的 Terminated 且 exit code=3 → 落盘 + 判定容器异常退出。
- **附加断言**：`/tmp/flowlogs` 下**产生** `flowtest-flow-c9-*.log`（含 sidecar ERROR 行）。

## 3.10 C10 重启次数超限（exit 3）

- **场景**：容器先就绪后以 **exit 0** 反复重启（`--max-restart 1`），RestartCount 累计超限。
- **验证链路**：`checkExit` 判定顺序中，exit 0 不命中 `containerExitCode`（非 0 才命中）分支 → 落到 `totalRestartCount > MaxRestart` 分支 → `exitCh <- -1` → `EventRestartLimit` 告警 → exit 3。
- **判定关键字**：`重启次数超过上限`。

## 3.11 C11 多副本聚合（exit 3，双落盘）

- **场景**：replicas=2，两副本均先就绪后以 exit 3 报错退出。
- **验证链路**：checker 对每个副本各起 goroutine 独立追踪 → 各自捕获异常退出、各自落盘 → `wg.Wait()` 聚合后统一以 exit 3 结束。
- **附加断言**：`/tmp/flowlogs` 下**产生 ≥ 2 个** `flowtest-flow-c11-*.log` 文件（每副本一个）。

## 3.12 C12 无错误关键字（exit 0，不落盘）

- **场景**：容器循环输出 `[INFO]`/`[WARN]` 行，指定 `--log-err-keywords ERROR,FATAL` 无命中。
- **验证链路**：recorder 环形缓冲 `lineRing` 持续入缓冲，全程未命中错误 → 收尾 `f == nil && buf.len() > 0` 查 Pod 状态，容器正常存活 → 缓冲丢弃，不落盘。
- **附加断言**：`/tmp/flowlogs` 下**不产生** `flowtest-flow-c12-*.log` 文件。

## 3.13 C13 命中错误 + ignore 混合（exit 0，落盘校验）

- **场景**：容器交替输出 `[FATAL] connection refused for host` 与 `[FATAL] this is a known issue, ignore it`，指定 `--log-err-keywords FATAL --log-ignore-keywords "known issue"`。
- **验证链路**：命中 `FATAL` 且非 ignore 的行 → errorHit + 落盘 + 标红；命中 `FATAL` 但含 ignore 关键字 `known issue` 的行 → 过滤不标红。
- **附加断言**：落盘文件**包含** `connection refused` 错误行（即仅含非 ignore 错误行）。

## 3.14 C14 阶段1 目标锁定超时（exit 2）

- **场景**：Deployment 正常部署收敛后缩容到 0（已收敛且无 Pod），工具启动后窗口内不发生任何更新。
- **验证链路**：阶段1 走"已收敛且无 Pod → 等待更新"分支，`--deploy-ready-timeout 10` 窗口内三分支均未满足 → 告警「未检测到更新或新目标 Pod」+ exit 2（不进入阶段2/3）。
- **判定关键字**：`未检测到更新`。
- **语义变化**：旧版本"未产生任何 Pod"为独立告警路径（exit 3）；新逻辑并入锁定超时语义（exit 2，预期变化）。

---

# 4. 测试过程中的关键问题与修复

## 4.1 阶段重构带来的用例语义变化

- **现象**：本次迭代将阶段1 从"等待滚动收敛"重构为"目标锁定"、阶段2 判定从 Running 升级为 Ready，两个用例的预期行为发生变化。
- **影响用例**：
  - C2（探针失败）：旧版 exit 2（收敛超时）→ 新版 exit 3（阶段2 就绪超时捕获，探针校验能力保留）；
  - C3：判定关键字 `未进入 running` → `未就绪`；旧版"未产生任何 Pod exit 3"路径并入 exit 2。
- **修复**：C2 断言改为 exit 3 + `未就绪`；新增 C14 验证 exit 2 新语义（锁定超时）。

## 4.2 LLM 仲裁的 e2e 策略

- **现象**：LLM 仲裁默认启用（智谱 GLM 默认值），e2e 若不显式禁用会真实调用云端 API：断言结果不确定（LLM 可能判定 ERROR 行为假错误，C5/C13 的「日志需检查」断言翻车）且消耗配额。
- **修复**：全部用例（`run_tool` 与 C3/C6/C7 直连命令）统一追加 `--llm-endpoint ""` 显式禁用，回退关键字即真的确定性路径；LLM 仲裁逻辑由单测覆盖（httptest fake server + fake judge + fake clientset 断言 SinceTime 透传）。

## 4.3 LLM 端点不可达时的 WARN 刷屏（熔断修复）

- **现象**：真实冒烟测试（WSL 无外网环境）中，LLM 调用立即失败（DNS 拒绝）导致每个攒批周期都重试并打印 WARN，20 秒窗口内产生 17 条告警日志。
- **修复**：新增 `arbBreaker` 熔断器——连续 3 次调用失败后熔断，后续命中行直接按关键字即真处理，不再发起网络调用；冒烟复测确认仅 2 条降级 WARN + 1 条熔断告警后完全静默，行为正确（降级为真错误 → 落盘 + warning + exit 0）。

## 4.4 controller 删除 Pod 的误报防护（逻辑补强）

- **现象**：目标锁定提前到滚动 surge 期间后，surge 临时副本会被纳入锁定名单，其后续被 controller 删除（SIGTERM，退出码 143）会被误判为容器异常退出。
- **修复**：阶段2 `waitPodReady` 对 NotFound / DeletionTimestamp 非空的 Pod 静默豁免；阶段3 退出检测对删除中的 Pod 豁免——controller 删除的 Pod 不计为应用故障。

## 4.5 Calico CNI token 过期导致 Pod 沙箱创建失败（历史遗留）

- **现象**：`Failed to create pod sandbox: ... Unauthorized`，Pod 无法创建，导致 e2e 用例卡在 rollout。
- **修复**：给 `calico-node` DaemonSet 挂载长期 token（主容器 + `install-cni` init 容器），滚动更新后 install-cni 写入的仍是长期 token。

## 4.6 测试脚本工程化

- **WSL 双环境**：Windows CMD 需 `wsl -d Ubuntu-22.04` 前缀；go 不在默认 PATH，用 `bash -lc` 登录 shell。
- **资源清理**：`cleanup_deploy` 等待 Pod 完全消失（Terminating 残留 Pod 的 phase 可能仍是 Running，会被阶段2 误判为正常 Pod）。
- **taint 处理**：`trap remove_taint EXIT TERM INT` 统一移除全局 taint，避免残留污染其他用例。

---

# 5. 单元测试与配套验证（本轮迭代）

| 项 | 结果 |
|---|---|
| `go build ./...` | 成功 |
| `go vet ./...` | 无问题 |
| `go test ./...`（全量单测，含新增 internal/llm） | 全部 ok（checker / cmd / feishu / llm / options / recorder / watcher） |
| `internal/checker` 新增/改造用例 | lockTargetPods 三分支（收敛立即锁 / 滚动中锁新 RS / obsGen 防误锁 / 超时）、waitPodReady（未就绪 / removed 豁免）、Run 新语义（锁定超时 exit 2） |
| `internal/llm` 新增用例 | 9 例：成功 / 批量拆分 / markdown 围栏 / HTTP 错误 / 长度不匹配 / 非法内容 / 超时 / 默认值 / Enabled 语义 |
| `internal/recorder` 新增用例 | fake judge 仲裁（真错误快照 / 假错误不落盘 / 失败降级 / 退出兜底 / 禁用回退 / nil judge）、SinceTime 透传断言、arbBreaker 熔断、containerStartTime、truncFile |
| LLM 真实 API 冒烟（Windows 侧 curl 智谱端点） | key/model 有效；响应 `choices[0].message.content` 带 ```json 围栏（parseJudgeContent 已兼容）；批量判定 `[true,false]` 正确 |
| LLM 不可达冒烟（WSL 无外网 + 真实集群） | 降级为关键字即真（落盘 + warning + exit 0）；熔断后 WARN 静默 |

---

# 6. 测试结论

1. **功能完整性**：14 个 E2E 用例覆盖了三阶段检查链路的全部核心分支——正常通过、阶段2 就绪超时（探针失败 / Pending 未调度）、容器异常退出、日志告警、中断信号、-A 监听模式、目标锁定超时新语义，以及日志落盘的四类关键路径（ignore 优先、多容器、MaxRestart、多副本聚合、无错误不落盘、错误+ignore 混合）。
2. **正确性**：退出码（0/1/2/3/130）与设计规格完全一致；两项预期语义变化（探针失败 exit 2→3、无 Pod 路径并入 exit 2）均有对应用例验证；落盘断言（产生/不产生/数量/内容）全部通过。
3. **稳定性**：完整回归 PASS=19 FAIL=0，原有 C1、C4~C13 无回归，新增 C2 新语义、C3 关键字更新、C14 全部通过。
4. **LLM 仲裁**：默认启用的仲裁链路通过单测（协议/解析/降级/熔断/快照）与真实 API 冒烟（协议兼容性、判定正确性）双重验证；e2e 覆盖禁用降级路径。
5. **遗留项**：测试资源（deployment/rs/pods）在 `flowtest` 命名空间保留供手动测试；taint 已自动移除，如需复现 C3 的 Pending 场景可手动执行 `kubectl taint nodes <node> flowtest=block:NoSchedule`。
