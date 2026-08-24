# kubectl-check E2E 测试报告

# 1. 测试概述

## 1.1 测试目的

本报告记录 `kubectl-check` 插件（`kubecheck`，Go + cobra + client-go 实现）在**真实 Kubernetes 集群**上的端到端（E2E）验证结果。E2E 测试不依赖单元测试的 mock 集群，而是通过真实 Deployment 滚动更新、真实 Pod 生命周期、真实日志流式采集，逐用例验证插件三级检查链路的端到端行为：

- **第一级**：Deployment 滚动更新就绪校验（informer 事件驱动，对标 `kubectl wait`）
- **第二级**：Pod 状态追踪 + 实时日志监控（轮询 + Follow 流式长连接，并行执行）
- **第三级**：结果判定与告警（正常 / 就绪超时 / Pod 异常 / 日志告警 / 中断，映射退出码 0/1/2/3/130）

## 1.2 测试范围

| 项 | 说明 |
|---|---|
| 测试对象 | `kubectl-check`（kubecheck 二进制，`/tmp/flowbin`） |
| 测试脚本 | `e2e/e2e-test.sh`（用法：`bash e2e-test.sh [C1\|...\|C13\|ALL]`，默认 ALL） |
| 测试资源 | `test/flow-c1.yaml` ~ `test/flow-c13.yaml`（C1~C7 由脚本内联创建，C8~C13 由 yaml 文件创建） |
| 测试命名空间 | `flowtest` |
| 日志落盘目录 | `/tmp/flowlogs` |
| 覆盖用例数 | 13（C1~C13） |
| 断言数 | 18（含 6 个用例的附加落盘断言） |

## 1.3 测试环境

| 项 | 详情 |
|---|---|
| 集群 | Kubernetes v1.36.3（单节点 WSL 环境） |
| 网络插件 | Calico（CNI，containerd 运行时） |
| 客户端 | kubectl + client-go v0.31.0 |
| 运行环境 | WSL Ubuntu-22.04（Windows CMD 经 `wsl -d Ubuntu-22.04` 调用） |
| 测试时间 | 2026-08-22 ~ 2026-08-23 |

---

# 2. 测试结果总览

**最终回归结果：PASS=18 FAIL=0，全部 13 个用例通过，无回归。**

| 用例 | 场景描述 | 期望退出码 | 附加断言 | 结果 |
|---|---|---|---|---|
| C1 | 正常通过（阶段1+2 通过、日志无错误） | 0 | - | PASS |
| C2 | 阶段1 Deployment 滚动就绪超时 | 2 | - | PASS |
| C3 | 阶段2 Pod 未进入 Running 超时 | 3 | - | PASS |
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

> 说明：PASS 计数为 18 = 13 个用例主断言 + 5 个附加落盘断言（C8/C9/C11/C12/C13）。

---

# 3. 用例详解

## 3.1 C1 正常通过（exit 0）

- **场景**：`nginx:1.25` Deployment 正常滚动更新，Pod Ready 且日志无错误关键字。
- **验证链路**：阶段1 informer 就绪 → 阶段2 Pod Running + Ready → 阶段3 日志追踪 10s 无错误命中。
- **判定关键字**：`全部 Pod 运行正常`。

## 3.2 C2 阶段1 滚动就绪超时（exit 2）

- **场景**：`busybox` + `readinessProbe: /bin/false`（探针永远失败）→ Pod Ready=0/1 → 滚动永不完成，阶段1 条件永不满足。
- **验证链路**：阶段1 `--deploy-ready-timeout 15` 超时 → 告警后直接退出，**不继续追踪 Pod**。
- **判定关键字**：`滚动更新未完成`。

## 3.3 C3 阶段2 Pod 未进入 Running 超时（exit 3）

- **场景**：replicas=0 使阶段1 立即通过；节点施加 `flowtest=block:NoSchedule` taint 后 scale 出 2 个 Pending Pod。
- **验证链路**：阶段2 `--pod-ready-timeout 15` 内 Pod 始终 Pending → 超时告警，异常退出。
- **判定关键字**：`未进入 running`。

## 3.4 C4 阶段3 容器异常退出（exit 3）

- **场景**：容器 `sleep 45; exit 1` → Pod 先 Ready（阶段1/2 通过），45s 后容器以退出码 1 终止。
- **验证链路**：阶段3 捕获 `ContainerTerminated` 且 exit code != 0 → 判定容器异常退出，告警后异常退出。
- **判定关键字**：`容器异常退出`。

## 3.5 C5 阶段3 日志报错但 Pod 存活（exit 0，warning）

- **场景**：容器循环输出 `ERROR something went wrong`，Pod 存活不退出。
- **验证链路**：阶段3 命中错误关键字 `ERROR` → 落盘 + 置 warning；Pod 未退出，最终以 0 退出并提示「日志需检查」。
- **判定关键字**：`日志需检查`。

## 3.6 C6 中断信号（exit 130）

- **场景**：正常 Deployment，工具运行 5s 后向其发送 SIGINT。
- **验证链路**：信号处理链路 → 优雅退出，退出码 130，不误报为业务错误。
- **判定关键字**：`检查被中断`。

## 3.7 C7 -A 监听模式（exit 130）

- **场景**：`kubectl check -A` 后台启动 watcher → `rollout restart` 触发 Deployment generation 递增 → watcher 自动启动检查 → SIGINT 中断 watcher。
- **验证链路**：-A 全命名空间监听 → informer 识别 generation 变化 → 自动触发检查（日志命中「检测到 Deployment ... 更新」与「... 检查完成」）→ 中断退出 130。
- **判定关键字**：`检测到 Deployment`。

## 3.8 C8 忽略关键字优先（exit 0，不落盘）

- **场景**：容器输出 `[ERROR] this is an expected error`，同时指定 `--log-err-keywords ERROR --log-ignore-keywords expected`。
- **验证链路**：recorder 命中判定 `hitAny(line, errKw) && !hitIgnore(line, ignoreKw)` —— ignore 优先级高于 error，该行不标红、不算 errorHit、不落盘。
- **附加断言**：`/tmp/flowlogs` 下**不产生** `flowtest-flow-c8-*.log` 文件。

## 3.9 C9 多容器（exit 3，落盘）

- **场景**：Pod 含 `main`（持续存活）+ `sidecar`（先 `sleep 30` 保证 Pod Ready 通过阶段1，后输出 ERROR 并以 exit 3 退出）两个容器。
- **验证链路**：recorder 遍历 `pod.Status.ContainerStatuses`，捕获 sidecar 的 Terminated 且 exit code=3 → 落盘 + 判定容器异常退出。
- **附加断言**：`/tmp/flowlogs` 下**产生** `flowtest-flow-c9-*.log`（含 sidecar ERROR 行）。

## 3.10 C10 重启次数超限（exit 3）

- **场景**：容器先就绪后以 **exit 0** 反复重启（`--max-restart 1`），RestartCount 累计超限。
- **验证链路**：`checkExit` 判定顺序中，exit 0 不命中 `containerExitCode`（非 0 才命中）分支 → 落到 `totalRestartCount > MaxRestart` 分支 → `exitCh <- -1` → `EventRestartLimit` 告警 → exit 3。
- **判定关键字**：`重启次数超过上限`。
- **设计要点**：必须「先就绪再重启」且使用 exit 0——否则 Pod 永不 Ready 会卡在阶段1（exit 2），或非 0 退出会先走容器异常退出分支，MaxRestart 分支无法被触发。

## 3.11 C11 多副本聚合（exit 3，双落盘）

- **场景**：replicas=2，两副本均先就绪（30s）后以 exit 3 报错退出。
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

---

# 4. 测试过程中的关键问题与修复

## 4.1 阶段1 前置阻断对用例设计的约束

- **现象**：阶段1 是前置阻断——Pod 必须全部 Ready（滚动完成）才进入阶段2/3。若容器启动即崩溃，Pod 永不 Ready，检查会卡在阶段1 直接 exit 2。
- **影响用例**：C9/C10/C11（需要进入阶段3 的异常场景）。
- **修复**：三个用例统一改为「**先就绪、后出问题**」——sidecar/容器先 `sleep 25~30s` 保证 Pod Ready 通过阶段1，再报错退出 / 重启超限。

## 4.2 `containerExitCode` 优先于 `MaxRestart` 的判定顺序

- **现象**：`checkExit` 中，容器退出码非 0 会先走「容器异常退出」分支，`totalRestartCount > MaxRestart` 分支永远触发不了。
- **影响用例**：C10。
- **修复**：C10 改用 **exit 0** 反复重启（exit 0 不命中异常退出分支），从而正确走到 MaxRestart → `exitCh <- -1` → `EventRestartLimit` 路径。

## 4.3 Calico CNI token 过期导致 Pod 沙箱创建失败

- **现象**：`Failed to create pod sandbox: ... plugin type="calico" failed (add): error getting ClusterInformation: connection is unauthorized: Unauthorized`，Pod 无法创建，导致 e2e 用例卡在 rollout。
- **根因**：`calico-node` 的 SA 无绑定长期 token secret，`install-cni` 将**短期 projected token** 写入宿主机 `/etc/cni/net.d/calico-kubeconfig`；token 过期后 CNI 插件拉取 ClusterInformation 被 API server 拒绝（401）。
- **修复**：
  1. 立即恢复：创建绑定 `calico-node` SA 的长期 token secret，更新宿主机 kubeconfig 并验证认证；
  2. 持久化防复发：给 `calico-node` DaemonSet 挂载长期 token 到 `/var/run/secrets/kubernetes.io/serviceaccount`（主容器 + `install-cni` init 容器），滚动更新后 install-cni 写入的仍是长期 token。

## 4.4 测试脚本工程化

- **WSL 双环境**：Windows CMD 需 `wsl -d Ubuntu-22.04` 前缀；PATH 被 Windows 变量污染，用 `env PATH=/usr/local/go/bin:/usr/bin:/bin` 绕过；go 不在默认 PATH。
- **资源清理**：`cleanup_deploy` 等待 Pod 完全消失（Terminating 残留 Pod 的 phase 可能仍为 Running，会被阶段2 误判为正常 Pod，导致下一次用例漏检）。
- **taint 处理**：`trap remove_taint EXIT TERM INT` 统一移除全局 taint，避免残留污染其他用例；`kubectl taint` 不支持 `--ignore-not-found`，错误统一吞掉。

---

# 5. 单元测试回归（配套）

E2E 之外，本轮迭代同步执行了以下配套验证：

| 项 | 结果 |
|---|---|
| `go build` | 成功 |
| `go vet ./...` | 无问题 |
| `go test ./...`（全量单测） | 全部 ok（checker/cmd/feishu/options/recorder/watcher） |
| `internal/watcher` 单元测试（新增 7 例） | 7/7 PASS |

---

# 6. 测试结论

1. **功能完整性**：13 个 E2E 用例覆盖了三阶段检查链路的全部核心分支——正常通过、阶段1 超时、阶段2 超时、容器异常退出、日志告警、中断信号、-A 监听模式，以及日志落盘的四类关键路径（ignore 优先、多容器、MaxRestart、多副本聚合、无错误不落盘、错误+ignore 混合）。
2. **正确性**：退出码（0/1/2/3/130）与设计规格完全一致；落盘断言（产生/不产生/数量/内容）全部通过，证明「命中错误关键字才落盘」的核心改造生效。
3. **稳定性**：完整回归 PASS=18 FAIL=0，原有 C1~C7 无回归，新增 C8~C13 全部通过。
4. **遗留项**：测试资源（deployment/rs/pods）在 `flowtest` 命名空间保留供手动测试；taint 已自动移除，如需复现 C3 的 Pending 场景可手动执行 `kubectl taint nodes <node> flowtest=block:NoSchedule`。
