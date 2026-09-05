#!/usr/bin/env bash
# kubectl-check 工具 E2E 测试脚本（真实 k8s 集群）
#
# 用法：bash e2e-test.sh [C1|C2|...|C18|ALL]   （默认 ALL）
#
# 前置：kubectl 可用、/tmp/flowbin 已构建、namespace flowtest 存在
#
# 当前核心逻辑（用例对应关系）：
#   目标定位：replicas=0 立即 exit 2；否则 pod-ready-timeout 窗口内轮询等
#            最新 RS 的目标 Pod 出现（Pending 即算定位成功）
#   阶段2/3 并行：waitFirstRunning 等首次 Running -> 并行启动
#            「waitReady 就绪等待 + watchPodLog 日志观察」，podCtx 联动取消
#   开关矩阵（默认值）：--log-enable=true  --log-console=true
#            --keyword-check=false  --llm-enable=false  --log-dump=false
#
# 覆盖用例：
#   C1  正常通过（默认参数）                                -> exit 0
#   C2  阶段2 未就绪（探针失败；并行期容器存活无退出误报）  -> exit 3
#   C3  阶段2 未就绪（Pending 未调度）                      -> exit 3
#   C4  阶段3 容器异常退出（Ready 后崩溃）                  -> exit 3
#   C5  阶段3 日志命中关键字但 Pod 存活（--keyword-check）   -> exit 0（warning）
#   C6  中断信号（SIGINT）                                  -> exit 130
#   C7  -A 监听模式：更新自动触发检查，中断 watcher         -> exit 130
#   C8  命中错误但被 ignore 抵消 -> 不落盘                  -> exit 0
#   C9  多容器：sidecar 报错退出 -> 落盘（--log-dump）      -> exit 3
#   C10 重启次数超限（--max-restart）                       -> exit 3
#   C11 多副本聚合（2 副本均报错退出）-> 各落盘             -> exit 3
#   C12 无错误关键字 -> 缓冲后丢弃不落盘                    -> exit 0
#   C13 错误 + ignore 混合 -> 落盘仅含非 ignore 行          -> exit 0
#   C14 目标定位无 Pod（replicas=0）-> 立即                 -> exit 2
#   C15 默认 --log-dump=false：容器异常退出告警但不落盘     -> exit 3
#   C16 阶段2/3 并行：0/1 Running 期间容器崩溃 -> 立即
#       「容器异常退出」且联动取消阶段2（无未就绪重复告警、
#       不等 pod-ready-timeout）                            -> exit 3
#   C17 --log-console 默认开：追踪日志实时输出控制台，
#       关键字判定默认关无提醒                              -> exit 0
#   C18 --log-enable=false：跳过阶段3，Ready 即完成
#       （后续容器崩溃不捕获、不等待）                      -> exit 0
#   C19 -A 监听模式：启动时存量 Deployment 不触发（存量抑制），
#       运行期新建 Deployment 触发检查                       -> watcher 130
#   C20 -n glob 监听模式：'-n *-prod' 仅匹配 prod 后缀命名空间，
#       匹配 ns 创建触发 / 非匹配 ns 更新不触发              -> watcher 130
#   C21 多副本滚动后置发现：2 副本 rollout restart 紧贴启动工具，
#       首发 Pod 完整检查（阶段2+3），后置 Pod 补录后仅就绪
#       检查（无日志观察）                                   -> exit 0
#   C22 滚动卡住：探针失败 + maxSurge=1/maxUnavailable=0 使第 2 个
#       新 Pod 永不创建 -> 后置发现窗口结束（1/2）静默打日志，
#       仅首发未就绪一条告警（无重复、不误报）              -> exit 3
#
# 注意1：LLM 仲裁默认关闭（--llm-enable 默认 false），需关键字断言的用例统一
#        --keyword-check 开启"关键字即真"判定，保证确定性且不消耗云端配额；
#        LLM 仲裁逻辑由单测（fake judge/httptest）覆盖。
# 注意2：用例结束后保留测试资源（deployment/rs/pods）不清理，供手动测试。
#        taint 不保留（全局的，残留会污染其他用例）；flow-c3 的 Pod 在 taint
#        移除后会被调度为 Running，如需复现可手动：
#        kubectl taint nodes <node> flowtest=block:NoSchedule
set -u

NS=flowtest
BIN=/tmp/flowbin
LOGDIR=/tmp/flowlogs
# 测试资源 yaml 目录（脚本位于 e2e/，资源位于 ../test/）
TESTDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/../test" && pwd)
mkdir -p "$LOGDIR"

NODE=$(kubectl get nodes -o jsonpath='{.items[0].metadata.name}')
PASS=0
FAIL=0
declare -a FAILED_CASES=()

# ---------- 辅助函数 ----------
cleanup_deploy() { # $1 deployment 名
  kubectl -n "$NS" delete deployment "$1" --wait=true --ignore-not-found=true >/dev/null 2>&1
  kubectl -n "$NS" delete rs,pods -l "app=$1" --force --grace-period=0 --ignore-not-found=true >/dev/null 2>&1
  # 等待 Pod 完全消失：Terminating 残留 Pod 的 phase 可能仍是 Running，
  # 会被工具阶段2 误判为"已就绪的正常 Pod"，导致下一次用例漏检真正异常的 Pod
  local n
  for _ in $(seq 1 60); do
    n=$(kubectl -n "$NS" get pods -l "app=$1" --no-headers 2>/dev/null | wc -l)
    [ "$n" -eq 0 ] && break
    sleep 1
  done
}

apply_taint() {
  kubectl taint nodes "$NODE" flowtest=block:NoSchedule --overwrite >/dev/null 2>&1
}

remove_taint() {
  # 注意：kubectl taint 不支持 --ignore-not-found（会直接报 unknown flag），
  # 不存在该 taint 时命令会报错，统一吞掉即可
  kubectl taint nodes "$NODE" flowtest=block:NoSchedule- >/dev/null 2>&1
}

check_result() { # $1 用例名(flow-CX) $2 实际退出码 $3 期望退出码 $4 期望输出关键字(可为空串)
  local name=$1 code=$2 want=$3 kw=$4
  if [ "$code" -ne "$want" ]; then
    echo "[FAIL] $name: exit=$code want=$want"
    FAIL=$((FAIL+1)); FAILED_CASES+=("$name(exit:$code/want:$want)")
    return
  fi
  if [ -n "$kw" ] && ! grep -q "$kw" "$LOGDIR/$name.out" 2>/dev/null; then
    echo "[FAIL] $name: exit=$code 但未找到输出关键字 [$kw]"
    FAIL=$((FAIL+1)); FAILED_CASES+=("$name(缺少输出[$kw])")
    return
  fi
  echo "[PASS] $name: exit=$code"
  PASS=$((PASS+1))
}

run_tool() { # $1 用例名 $2... 工具参数；-v 输出实时打印（走 stderr，避免污染命令替换）并落盘 $LOGDIR/$1.out
              # 统一 --log-dump 开启落盘（默认已关闭）供 C9/C11/C13 的落盘断言；
              # 统一 --keyword-check 开启关键字即真判定（--llm-enable 默认 false 不做仲裁）；
              # --log-console 默认开启（追踪日志实时上屏，不影响断言）
  local name=$1; shift
  timeout 300 "$BIN" deployment/"$name" -n "$NS" \
    --log-error-dir "$LOGDIR" --log-dump --keyword-check -v "$@" 2>&1 | tee "$LOGDIR/$name.out" >&2
  echo ${PIPESTATUS[0]}
}

assert_absent() { # $1 用例名 $2 不应出现的关键字 $3 失败说明
  if grep -q "$2" "$LOGDIR/$1.out" 2>/dev/null; then
    echo "[FAIL] $1: $3"
    FAIL=$((FAIL+1)); FAILED_CASES+=("$1($2)")
    return 1
  fi
  return 0
}

# 测试资源（deployment/rs/pods）保留不清理；taint 是全局的，退出时统一移除，
# 避免残留污染其他用例 / 用户后续手动测试。重复运行同一用例时，
# 用例开头的 cleanup_deploy 会先重建同名资源。
trap remove_taint EXIT TERM INT

# ---------- 用例 ----------
run_c1() { # 正常通过（默认参数：关键字/LLM/落盘全关，仅就绪+退出检测）-> exit 0
  local C=c1
  echo "---- [$C] 正常通过 ----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=nginx:1.25 -n "$NS" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --pod-ready-timeout 30 --log-check-timeout 10)
  check_result "flow-$C" "$code" 0 "全部 Pod 运行正常"
}

run_c2() { # 阶段2 未就绪（探针失败）；阶段3 并行运行但容器存活 -> 仅「未就绪」一条告警
  local C=c2
  echo "---- [$C] 阶段2 未就绪（探针失败）----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=busybox:1.36 -n "$NS" -- /bin/sh -c "sleep 3600" >/dev/null 2>&1
  # exec 探针永远失败 -> Pod 永不 Ready（0/1 Running）；目标定位不看收敛，
  # 阶段2 就绪超时捕获；并行期阶段3 退出检测运行但容器存活（sleep 3600）
  kubectl -n "$NS" patch deployment "flow-$C" -p \
    '{"spec":{"template":{"spec":{"containers":[{"name":"busybox","readinessProbe":{"exec":{"command":["/bin/false"]},"periodSeconds":2,"failureThreshold":3}}]}}}}' >/dev/null 2>&1
  kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Running pod -l "app=flow-$C" --timeout=60s >/dev/null 2>&1
  sleep 3
  local code
  code=$(run_tool "flow-$C" --pod-ready-timeout 15 --log-check-timeout 5)
  check_result "flow-$C" "$code" 3 "未就绪"
  # 附加断言：容器存活不应触发「容器异常退出」（并行期无退出误报）
  if assert_absent "flow-$C" "容器异常退出" "容器存活的未就绪场景不应报容器退出"; then
    echo "[PASS] flow-$C: 并行期无容器退出误报"
    PASS=$((PASS+1))
  fi
}

run_c3() { # 阶段2 未就绪（Pending 未调度）：目标定位含 Pending，就绪超时 -> exit 3
  local C=c3
  echo "---- [$C] 阶段2 未就绪（Pending）----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=busybox:1.36 -n "$NS" -- /bin/sh -c "sleep 3600" >/dev/null 2>&1
  # replicas=0 先收敛，避免 Pod 提前创建
  kubectl -n "$NS" scale deployment "flow-$C" --replicas=0 >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=60s >/dev/null 2>&1
  apply_taint
  # taint 阻挡调度下 scale 出 2 个 Pending Pod，等 Pod 创建完成后启动工具：
  # 目标定位轮询等到 2 个 Pending Pod（Pending 即算定位成功）-> 阶段2 就绪超时 -> exit 3
  kubectl -n "$NS" scale deployment "flow-$C" --replicas=2 >/dev/null 2>&1
  kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Pending pod -l "app=flow-$C" --timeout=60s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --pod-ready-timeout 15 --log-check-timeout 5)
  check_result "flow-$C" "$code" 3 "未就绪"
  remove_taint
}

run_c4() { # 阶段3 容器异常退出（无探针：秒 Ready 后崩溃，退出检测捕获）-> exit 3
  local C=c4
  echo "---- [$C] 阶段3 容器异常退出 ----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=busybox:1.36 -n "$NS" -- /bin/sh -c "sleep 45; exit 1" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=90s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --pod-ready-timeout 30 --log-check-timeout 120)
  check_result "flow-$C" "$code" 3 "容器异常退出"
}

run_c5() { # 阶段3 日志报错但存活（--keyword-check 命中即真）-> exit 0（warning）
  local C=c5
  echo "---- [$C] 阶段3 日志报错但 Pod 存活 ----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=busybox:1.36 -n "$NS" -- /bin/sh -c "while true; do echo 'ERROR something went wrong'; sleep 2; done" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=90s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --pod-ready-timeout 30 --log-check-timeout 15)
  check_result "flow-$C" "$code" 0 "日志需检查"
}

run_c6() { # 中断信号（SIGINT）-> exit 130
  local C=c6
  echo "---- [$C] 中断信号（SIGINT）----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=busybox:1.36 -n "$NS" -- /bin/sh -c "sleep 3600" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=90s >/dev/null 2>&1
  # 工具直接后台运行，$! 即工具 PID，便于向其发送 SIGINT；
  # 用 process substitution 实时打印并落盘，避免管道使 $! 变成 tee 的 PID
  "$BIN" deployment/"flow-$C" -n "$NS" \
    --log-error-dir "$LOGDIR" --keyword-check -v \
    --pod-ready-timeout 30 --log-check-timeout 300 \
    > >(tee "$LOGDIR/flow-$C.out") 2>&1 &
  local tool_pid=$!
  sleep 5
  kill -INT "$tool_pid" 2>/dev/null
  wait "$tool_pid"
  local code=$?
  check_result "flow-$C" "$code" 130 "检查被中断"
}

run_c7() { # -A 监听模式：deployment 更新自动触发检查，SIGINT 中断 watcher -> exit 130
  local C=c7
  echo "---- [$C] -A 监听模式：更新自动触发检查 ----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=nginx:1.25 -n "$NS" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  # 后台启动 -A watcher（不指定资源；监听所有 namespace 的 deployment）。
  # 与 C6 相同的 process substitution 方式，$! 即工具 PID
  "$BIN" -A \
    --log-error-dir "$LOGDIR" --keyword-check -v \
    --pod-ready-timeout 30 --log-check-timeout 10 \
    > >(tee "$LOGDIR/flow-$C.out") 2>&1 &
  local watch_pid=$!
  # 等待 watcher 就绪（informer 同步完成、打印启动日志）
  local ready=0
  for _ in $(seq 1 15); do
    if grep -q "已启动" "$LOGDIR/flow-$C.out" 2>/dev/null; then ready=1; break; fi
    sleep 1
  done
  if [ "$ready" -ne 1 ]; then
    echo "[FAIL] flow-$C: -A 监听未在预期时间内启动"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(监听未启动)")
    kill -INT "$watch_pid" 2>/dev/null
    wait "$watch_pid" 2>/dev/null
    return
  fi
  # 触发更新：rollout restart 修改 pod-template annotation -> generation 递增，
  # 应被 watcher 识别并自动启动现有检查逻辑
  kubectl -n "$NS" rollout restart deployment/"flow-$C" >/dev/null 2>&1
  # 等待自动检查完成（同时命中"检测到更新"与"检查完成"日志）
  local done=0
  for _ in $(seq 1 90); do
    if grep -q "检测到 Deployment flowtest/flow-$C 更新" "$LOGDIR/flow-$C.out" 2>/dev/null \
      && grep -q "flowtest/flow-$C 检查完成" "$LOGDIR/flow-$C.out" 2>/dev/null; then
      done=1; break
    fi
    sleep 1
  done
  if [ "$done" -ne 1 ]; then
    echo "[FAIL] flow-$C: deployment 更新后未自动触发检查"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(未自动触发)")
    kill -INT "$watch_pid" 2>/dev/null
    wait "$watch_pid" 2>/dev/null
    return
  fi
  # SIGINT 中断 watcher -> exit 130（与 C6 同一信号处理链路）
  kill -INT "$watch_pid" 2>/dev/null
  wait "$watch_pid"
  local code=$?
  check_result "flow-$C" "$code" 130 "检测到 Deployment"
}

run_c8() { # 命中错误关键字但被 ignore 抵消 -> 不落盘，exit 0
  local C=c8
  echo "---- [$C] 忽略关键字优先（ERROR 被 ignore）----"
  cleanup_deploy "flow-$C"
  kubectl apply -f "$TESTDIR/flow-$C.yaml" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --pod-ready-timeout 30 --log-check-timeout 12 \
    --log-err-keywords ERROR --log-ignore-keywords expected)
  check_result "flow-$C" "$code" 0 "全部 Pod 运行正常"
  # 附加断言：命中 ignore 的错误行不落盘
  if ls "$LOGDIR"/flowtest-flow-$C-*.log >/dev/null 2>&1; then
    echo "[FAIL] flow-$C: 命中 ignore 不应产生日志文件"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(不应落盘但已落盘)")
  else
    echo "[PASS] flow-$C: 未产生日志文件（ignore 生效）"
    PASS=$((PASS+1))
  fi
}

run_c9() { # 多容器：sidecar 先就绪后报错退出 -> 落盘（--log-dump），exit 3
  local C=c9
  echo "---- [$C] 多容器（sidecar 先就绪后报错退出）----"
  cleanup_deploy "flow-$C"
  kubectl apply -f "$TESTDIR/flow-$C.yaml" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --pod-ready-timeout 30 --log-check-timeout 90)
  check_result "flow-$C" "$code" 3 "容器异常退出"
  # 附加断言：sidecar 的 ERROR 行已落盘
  if ! ls "$LOGDIR"/flowtest-flow-$C-*.log >/dev/null 2>&1; then
    echo "[FAIL] flow-$C: 容器异常退出应产生日志文件"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(应落盘但未落盘)")
  else
    echo "[PASS] flow-$C: 已产生日志文件（多容器落盘）"
    PASS=$((PASS+1))
  fi
}

run_c10() { # 重启次数超限（--max-restart）-> exit 3
  local C=c10
  echo "---- [$C] 重启次数超限（MaxRestart）----"
  cleanup_deploy "flow-$C"
  kubectl apply -f "$TESTDIR/flow-$C.yaml" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --pod-ready-timeout 30 --log-check-timeout 120 \
    --max-restart 1)
  check_result "flow-$C" "$code" 3 "重启次数超过上限"
}

run_c11() { # 多副本聚合：2 副本均先就绪后报错退出 -> 各落盘一个文件，exit 3
  local C=c11
  echo "---- [$C] 多副本聚合（2 副本均报错退出）----"
  cleanup_deploy "flow-$C"
  kubectl apply -f "$TESTDIR/flow-$C.yaml" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --pod-ready-timeout 30 --log-check-timeout 90)
  check_result "flow-$C" "$code" 3 "容器异常退出"
  # 附加断言：2 副本各落盘一个日志文件
  local nlog
  nlog=$(ls "$LOGDIR"/flowtest-flow-$C-*.log 2>/dev/null | wc -l)
  if [ "$nlog" -lt 2 ]; then
    echo "[FAIL] flow-$C: 2 副本均报错应产生 2 个日志文件（实际 $nlog）"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(落盘文件数不足:$nlog)")
  else
    echo "[PASS] flow-$C: 2 副本各落盘一个日志文件"
    PASS=$((PASS+1))
  fi
}

run_c12() { # 无错误关键字（仅 INFO/WARN）-> 缓冲后丢弃不落盘，exit 0
  local C=c12
  echo "---- [$C] 无错误关键字（仅 INFO/WARN）----"
  cleanup_deploy "flow-$C"
  kubectl apply -f "$TESTDIR/flow-$C.yaml" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --pod-ready-timeout 30 --log-check-timeout 12 \
    --log-err-keywords "ERROR,FATAL")
  check_result "flow-$C" "$code" 0 "全部 Pod 运行正常"
  # 附加断言：无错误命中不落盘
  if ls "$LOGDIR"/flowtest-flow-$C-*.log >/dev/null 2>&1; then
    echo "[FAIL] flow-$C: 无错误关键字不应产生日志文件"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(不应落盘但已落盘)")
  else
    echo "[PASS] flow-$C: 未产生日志文件（无错误命中）"
    PASS=$((PASS+1))
  fi
}

run_c13() { # 命中错误 + ignore 混合 -> 落盘仅含非 ignore 错误行，exit 0
  local C=c13
  echo "---- [$C] 命中错误 + ignore 混合 ----"
  cleanup_deploy "flow-$C"
  kubectl apply -f "$TESTDIR/flow-$C.yaml" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --pod-ready-timeout 30 --log-check-timeout 12 \
    --log-err-keywords FATAL --log-ignore-keywords "known issue")
  check_result "flow-$C" "$code" 0 "日志需检查"
  # 附加断言：落盘文件包含非 ignore 的错误行
  local logfile
  logfile=$(ls "$LOGDIR"/flowtest-flow-$C-*.log 2>/dev/null | head -1)
  if [ -z "$logfile" ]; then
    echo "[FAIL] flow-$C: 命中错误关键字应产生日志文件"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(应落盘但未落盘)")
  elif ! grep -q "connection refused" "$logfile"; then
    echo "[FAIL] flow-$C: 落盘文件应包含非 ignore 的错误行"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(落盘内容缺失)")
  else
    echo "[PASS] flow-$C: 落盘文件包含非 ignore 错误行"
    PASS=$((PASS+1))
  fi
}

run_c14() { # 目标定位无 Pod（replicas=0，无目标可等）-> 立即 exit 2
  local C=c14
  echo "---- [$C] 目标定位无 Pod ----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=nginx:1.25 -n "$NS" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  # 收敛后缩容到 0：无目标可等（controller 不会创建 Pod），应立即告警 exit 2
  kubectl -n "$NS" scale deployment "flow-$C" --replicas=0 >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=60s >/dev/null 2>&1
  local start=$(date +%s)
  local code
  code=$(run_tool "flow-$C" --pod-ready-timeout 15 --log-check-timeout 5)
  local elapsed=$(( $(date +%s) - start ))
  check_result "flow-$C" "$code" 2 "未找到目标 Pod"
  # 附加断言：replicas=0 立即判定，不应轮询等待
  if [ "$elapsed" -le 10 ]; then
    echo "[PASS] flow-$C: ${elapsed}s 立即退出（未轮询等待）"
    PASS=$((PASS+1))
  else
    echo "[FAIL] flow-$C: 耗时 ${elapsed}s，疑似轮询等待了"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(耗时${elapsed}s)")
  fi
}

run_c15() { # 默认 --log-dump=false：容器异常退出 -> 告警正常但不落盘 -> exit 3
  local C=c15
  echo "---- [$C] 默认不落盘（--log-dump=false）----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=busybox:1.36 -n "$NS" -- /bin/sh -c "sleep 45; exit 1" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=90s >/dev/null 2>&1
  # 不走 run_tool（其统一开启 --log-dump/--keyword-check），直接调用验证默认行为
  timeout 300 "$BIN" deployment/"flow-$C" -n "$NS" \
    --log-error-dir "$LOGDIR" -v \
    --pod-ready-timeout 30 --log-check-timeout 120 \
    2>&1 | tee "$LOGDIR/flow-$C.out" >&2
  local code=${PIPESTATUS[0]}
  check_result "flow-$C" "$code" 3 "容器异常退出"
  # 附加断言：默认不产生日志文件，且告警不携带落盘路径
  if ls "$LOGDIR"/flowtest-flow-$C-*.log >/dev/null 2>&1; then
    echo "[FAIL] flow-$C: 默认不应落盘但已落盘"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(默认不应落盘但已落盘)")
  elif grep -q "日志已落盘" "$LOGDIR/flow-$C.out" 2>/dev/null; then
    echo "[FAIL] flow-$C: 默认不落盘时告警不应携带落盘路径"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(告警不应带落盘路径)")
  else
    echo "[PASS] flow-$C: 默认不落盘，告警不含落盘路径"
    PASS=$((PASS+1))
  fi
}

run_c16() { # 阶段2/3 并行：0/1 Running（探针未过）期间容器崩溃 ->
            # 阶段3 立即「容器异常退出」并联动取消阶段2（无未就绪重复告警、不等就绪超时）
  local C=c16
  echo "---- [$C] Ready 前容器崩溃（阶段2/3 并行捕获）----"
  cleanup_deploy "flow-$C"
  # 容器 15s 后 exit 1；探针永远失败 -> Pod 一直 0/1 Running（探针未过先崩）
  kubectl create deployment "flow-$C" --image=busybox:1.36 -n "$NS" -- /bin/sh -c "sleep 15; exit 1" >/dev/null 2>&1
  kubectl -n "$NS" patch deployment "flow-$C" -p \
    '{"spec":{"template":{"spec":{"containers":[{"name":"busybox","readinessProbe":{"exec":{"command":["/bin/false"]},"periodSeconds":2,"failureThreshold":3}}]}}}}' >/dev/null 2>&1
  # 等新 RS 的 Pod 进入 Running（0/1，探针失败）
  sleep 3
  kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Running pod -l "app=flow-$C" --timeout=60s >/dev/null 2>&1
  local start=$(date +%s)
  local code
  code=$(run_tool "flow-$C" --pod-ready-timeout 60 --log-check-timeout 60)
  local elapsed=$(( $(date +%s) - start ))
  check_result "flow-$C" "$code" 3 "容器异常退出"
  # 附加断言1：无「未就绪」重复告警（podCtx 联动取消阶段2，同一故障仅一条告警）
  if assert_absent "flow-$C" "未就绪" "Ready 前崩溃应只报容器异常退出"; then
    echo "[PASS] flow-$C: 无未就绪重复告警（podCtx 联动）"
    PASS=$((PASS+1))
  fi
  # 附加断言2：崩溃即退出（~20s），未等待 pod-ready-timeout=60s 的就绪超时
  if [ "$elapsed" -lt 45 ]; then
    echo "[PASS] flow-$C: ${elapsed}s 内结束（未等就绪超时）"
    PASS=$((PASS+1))
  else
    echo "[FAIL] flow-$C: 耗时 ${elapsed}s，疑似等待了就绪超时"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(耗时${elapsed}s疑等满超时)")
  fi
}

run_c17() { # --log-console 默认开：追踪日志实时输出控制台（同 kubectl logs -f）；
            # 关键字判定默认关 -> 无「日志需检查」提醒
  local C=c17
  echo "---- [$C] 日志实时输出控制台（--log-console 默认开）----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=busybox:1.36 -n "$NS" \
    -- /bin/sh -c "while true; do echo CONSOLE-MARK-LINE; sleep 1; done" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=90s >/dev/null 2>&1
  # 不走 run_tool（其统一开启 --keyword-check）：默认参数下 console 开、判定关
  timeout 300 "$BIN" deployment/"flow-$C" -n "$NS" -v \
    --pod-ready-timeout 30 --log-check-timeout 12 \
    2>&1 | tee "$LOGDIR/flow-$C.out" >&2
  local code=${PIPESTATUS[0]}
  check_result "flow-$C" "$code" 0 "全部 Pod 运行正常"
  # 附加断言1：追踪日志已逐行实时输出到控制台
  if grep -q "CONSOLE-MARK-LINE" "$LOGDIR/flow-$C.out" 2>/dev/null; then
    echo "[PASS] flow-$C: 追踪日志已实时输出到控制台"
    PASS=$((PASS+1))
  else
    echo "[FAIL] flow-$C: 控制台未见追踪日志输出"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(控制台无日志输出)")
  fi
  # 附加断言2：关键字判定默认关闭 -> 输出行不触发「日志需检查」
  if assert_absent "flow-$C" "日志需检查" "关键字判定默认关闭不应触发提醒"; then
    echo "[PASS] flow-$C: 关键字判定关闭，无提醒"
    PASS=$((PASS+1))
  fi
}

run_c18() { # --log-enable=false：跳过阶段3，Ready 即完成
            #（后续容器崩溃不捕获，不等待观察窗口）
  local C=c18
  echo "---- [$C] 禁用日志监控（--log-enable=false）----"
  cleanup_deploy "flow-$C"
  # 容器 45s 后 exit 1，但阶段3 已禁用：Ready 完成即退出，不捕获崩溃
  kubectl create deployment "flow-$C" --image=busybox:1.36 -n "$NS" -- /bin/sh -c "sleep 45; exit 1" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=90s >/dev/null 2>&1
  local start=$(date +%s)
  local code
  code=$(run_tool "flow-$C" --log-enable=false --pod-ready-timeout 30 --log-check-timeout 120)
  local elapsed=$(( $(date +%s) - start ))
  check_result "flow-$C" "$code" 0 "全部 Pod 运行正常"
  # 附加断言：Ready 即完成（~10s），未等待 log-check-timeout=120s 窗口
  if [ "$elapsed" -lt 25 ]; then
    echo "[PASS] flow-$C: ${elapsed}s 即退出（Ready 完成即结束，不观察日志）"
    PASS=$((PASS+1))
  else
    echo "[FAIL] flow-$C: 耗时 ${elapsed}s，疑似仍在观察日志"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(耗时${elapsed}s)")
  fi
}

run_c19() { # -A 监听模式：启动时存量 Deployment 不触发（存量抑制），
            # 运行期新建 Deployment 触发自动检查 -> 中断 watcher exit 130
  local C=c19
  echo "---- [$C] -A 监听模式：新建触发 + 存量抑制 ----"
  cleanup_deploy "flow-$C"
  # 后台启动 -A watcher（集群已有大量 flow-cX 存量 Deployment，可验证存量抑制）
  "$BIN" -A \
    --log-error-dir "$LOGDIR" -v \
    --pod-ready-timeout 30 --log-check-timeout 10 \
    > >(tee "$LOGDIR/flow-$C.out") 2>&1 &
  local watch_pid=$!
  local ready=0
  for _ in $(seq 1 15); do
    if grep -q "监听模式已启动" "$LOGDIR/flow-$C.out" 2>/dev/null; then ready=1; break; fi
    sleep 1
  done
  if [ "$ready" -ne 1 ]; then
    echo "[FAIL] flow-$C: -A 监听未在预期时间内启动"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(监听未启动)")
    kill -INT "$watch_pid" 2>/dev/null; wait "$watch_pid" 2>/dev/null
    return
  fi
  # 附加断言1：启动 10 秒内无任何存量 Deployment 触发（存量抑制）
  sleep 10
  local legacy
  legacy=$(grep -ac "启动检查" "$LOGDIR/flow-$C.out" 2>/dev/null || true)
  if [ "${legacy:-0}" -gt 0 ]; then
    echo "[FAIL] flow-$C: 启动时存量 Deployment 触发了检查（存量抑制失效）"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(存量误触发)")
  else
    echo "[PASS] flow-$C: 存量 Deployment 未触发（存量抑制生效）"
    PASS=$((PASS+1))
  fi
  # 运行期新建 Deployment -> 触发自动检查（generation=1 也触发）
  kubectl create deployment "flow-$C" --image=nginx:1.25 -n "$NS" >/dev/null 2>&1
  local done=0
  for _ in $(seq 1 90); do
    if grep -q "检测到 Deployment $NS/flow-$C 创建" "$LOGDIR/flow-$C.out" 2>/dev/null \
      && grep -q "$NS/flow-$C 检查完成" "$LOGDIR/flow-$C.out" 2>/dev/null; then
      done=1; break
    fi
    sleep 1
  done
  if [ "$done" -ne 1 ]; then
    echo "[FAIL] flow-$C: 新建 Deployment 未自动触发检查"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(新建未触发)")
    kill -INT "$watch_pid" 2>/dev/null; wait "$watch_pid" 2>/dev/null
    return
  fi
  echo "[PASS] flow-$C: 新建 Deployment 触发自动检查并完成"
  PASS=$((PASS+1))
  kill -INT "$watch_pid" 2>/dev/null
  wait "$watch_pid"
  local code=$?
  check_result "flow-$C" "$code" 130 "监听模式已启动"
  # 保留测试资源供手动测试
}

run_c20() { # -n glob 监听模式：'-n *-prod' 仅匹配 prod 后缀命名空间；
            # 非匹配 ns 更新不触发、匹配 ns 创建触发 -> 中断 watcher exit 130
  local C=c20
  echo "---- [$C] -n glob 监听模式（*-prod）----"
  local WNS=e2e-ns-prod
  cleanup_deploy "flow-$C"
  # 准备临时 ns（幂等）
  kubectl get ns "$WNS" >/dev/null 2>&1 || kubectl create ns "$WNS" >/dev/null 2>&1
  # 后台启动 watcher：-n '*-prod'（单引号防 shell glob 展开）
  "$BIN" -n '*-prod' \
    --log-error-dir "$LOGDIR" -v \
    --pod-ready-timeout 30 --log-check-timeout 5 \
    > >(tee "$LOGDIR/flow-$C.out") 2>&1 &
  local watch_pid=$!
  local ready=0
  for _ in $(seq 1 15); do
    if grep -q "监听模式已启动" "$LOGDIR/flow-$C.out" 2>/dev/null; then ready=1; break; fi
    sleep 1
  done
  if [ "$ready" -ne 1 ]; then
    echo "[FAIL] flow-$C: -n 监听未在预期时间内启动"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(监听未启动)")
    kill -INT "$watch_pid" 2>/dev/null; wait "$watch_pid" 2>/dev/null
    kubectl delete ns "$WNS" >/dev/null 2>&1
    return
  fi
  # 附加断言1：启动日志标明命名空间过滤范围
  if grep -q "命名空间过滤 \[\*-prod\]" "$LOGDIR/flow-$C.out" 2>/dev/null; then
    echo "[PASS] flow-$C: 启动日志标明过滤范围 *-prod"
    PASS=$((PASS+1))
  else
    echo "[FAIL] flow-$C: 启动日志未标明过滤范围"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(过滤范围未标注)")
  fi
  # 附加断言2：非匹配命名空间（flowtest，不匹配 *-prod）更新不触发
  kubectl -n "$NS" rollout restart deployment/flow-c19 >/dev/null 2>&1
  sleep 8
  if grep -q "检测到 Deployment $NS/" "$LOGDIR/flow-$C.out" 2>/dev/null; then
    echo "[FAIL] flow-$C: 非匹配命名空间的更新不应触发"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(非匹配ns误触发)")
  else
    echo "[PASS] flow-$C: 非匹配命名空间（flowtest）更新未触发"
    PASS=$((PASS+1))
  fi
  # 附加断言3：匹配命名空间（e2e-ns-prod）新建触发自动检查
  kubectl create deployment "flow-$C" --image=nginx:1.25 -n "$WNS" >/dev/null 2>&1
  local done=0
  for _ in $(seq 1 90); do
    if grep -q "检测到 Deployment $WNS/flow-$C 创建" "$LOGDIR/flow-$C.out" 2>/dev/null \
      && grep -q "$WNS/flow-$C 检查完成" "$LOGDIR/flow-$C.out" 2>/dev/null; then
      done=1; break
    fi
    sleep 1
  done
  if [ "$done" -ne 1 ]; then
    echo "[FAIL] flow-$C: 匹配命名空间的新建未触发检查"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(匹配ns未触发)")
  else
    echo "[PASS] flow-$C: 匹配命名空间（$WNS）新建触发自动检查并完成"
    PASS=$((PASS+1))
  fi
  kill -INT "$watch_pid" 2>/dev/null
  wait "$watch_pid"
  local code=$?
  check_result "flow-$C" "$code" 130 "监听模式已启动"
  # 清理临时 ns（含其中的 deployment/rs/pod）
  kubectl delete ns "$WNS" >/dev/null 2>&1
}

run_c21() { # 多副本滚动后置发现：2 副本 rollout restart 紧贴启动工具 ->
            # 首发 Pod 完整检查（阶段2+3），后置 Pod 补录后仅就绪检查（无日志观察）-> exit 0
  local C=c21
  echo "---- [$C] 多副本滚动后置发现 ----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=busybox:1.36 -n "$NS" --replicas=2 \
    -- /bin/sh -c "sleep 3600" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  # 紧贴 restart 启动工具：默认滚动策略 maxSurge≈1/maxUnavailable≈0（2 副本），
  # 新 RS 的 2 个 Pod 逐个创建——首发定位只拿到第 1 个（完整检查），
  # 第 2 个由后置发现器补录（仅就绪检查，无日志观察）
  kubectl -n "$NS" rollout restart deployment/"flow-$C" >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --pod-ready-timeout 60 --log-check-timeout 10)
  check_result "flow-$C" "$code" 0 "全部 Pod 运行正常"
  # 附加断言：后置发现生效（第 2 个新 Pod 被补录，仅就绪检查）；
  # 若启动时 2 个新 Pod 均已出现（时序抖动），则首发即双 Pod 完整检查，同样正确
  if grep -q "后置发现: 新目标 Pod" "$LOGDIR/flow-$C.out" 2>/dev/null; then
    echo "[PASS] flow-$C: 后置 Pod 已被补录跟踪（仅就绪检查）"
    PASS=$((PASS+1))
  elif grep -q "已定位 2 个目标 Pod" "$LOGDIR/flow-$C.out" 2>/dev/null; then
    echo "[PASS] flow-$C: 启动时 2 个新 Pod 均已出现（首发完整检查，无需补录）"
    PASS=$((PASS+1))
  else
    echo "[FAIL] flow-$C: 未见后置发现/双首发定位日志"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(后置发现无痕迹)")
  fi
}

run_c22() { # 滚动卡住：探针失败 + maxSurge=1/maxUnavailable=0 使第 2 个新 Pod 永不创建 ->
            # 后置发现窗口结束（已跟踪 1/2）静默打日志，仅首发未就绪一条告警 -> exit 3
  local C=c22
  echo "---- [$C] 滚动卡住（后置 Pod 不出现）----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=busybox:1.36 -n "$NS" --replicas=2 \
    -- /bin/sh -c "sleep 3600" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  # patch 探针失败 + 固定滚动策略（2 副本：maxSurge=1/maxUnavailable=0）：
  # 新 RS 仅创建 1 个 Pod 且永不 Ready -> 第 2 个新 Pod 永不出现（滚动卡住）
  kubectl -n "$NS" patch deployment "flow-$C" -p \
    '{"spec":{"strategy":{"rollingUpdate":{"maxSurge":1,"maxUnavailable":0}},"template":{"spec":{"containers":[{"name":"busybox","readinessProbe":{"exec":{"command":["/bin/false"]},"periodSeconds":2,"failureThreshold":3}}]}}}}' >/dev/null 2>&1
  # 等新 RS 的第 1 个 Pod 进入 Running（0/1 探针失败）
  sleep 5
  kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Running pod -l "app=flow-$C" --timeout=60s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --pod-ready-timeout 15 --log-check-timeout 5)
  check_result "flow-$C" "$code" 3 "未就绪"
  # 附加断言1：后置发现窗口结束日志（滚动卡住可观测，但不告警不置额外失败）
  if grep -q "后置发现: 窗口结束" "$LOGDIR/flow-$C.out" 2>/dev/null; then
    echo "[PASS] flow-$C: 窗口结束日志已打印（滚动卡住可观测）"
    PASS=$((PASS+1))
  else
    echo "[FAIL] flow-$C: 未见窗口结束日志"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(窗口结束日志缺失)")
  fi
  # 附加断言2：告警仅 1 条（首发未就绪；后置 Pod 不存在，无第二条告警、无滚动卡住误报）
  local n
  n=$(grep -ac "\[ALERT\]" "$LOGDIR/flow-$C.out" 2>/dev/null || true)
  if [ "${n:-0}" -eq 1 ]; then
    echo "[PASS] flow-$C: 告警仅 1 条（无重复、无滚动卡住误报）"
    PASS=$((PASS+1))
  else
    echo "[FAIL] flow-$C: 告警 ${n:-0} 条，应为 1"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(告警${n}条)")
  fi
}

# ---------- 主入口 ----------
case "${1:-ALL}" in
  c1|C1) run_c1 ;;
  c2|C2) run_c2 ;;
  c3|C3) run_c3 ;;
  c4|C4) run_c4 ;;
  c5|C5) run_c5 ;;
  c6|C6) run_c6 ;;
  c7|C7) run_c7 ;;
  c8|C8) run_c8 ;;
  c9|C9) run_c9 ;;
  c10|C10) run_c10 ;;
  c11|C11) run_c11 ;;
  c12|C12) run_c12 ;;
  c13|C13) run_c13 ;;
  c14|C14) run_c14 ;;
  c15|C15) run_c15 ;;
  c16|C16) run_c16 ;;
  c17|C17) run_c17 ;;
  c18|C18) run_c18 ;;
  c19|C19) run_c19 ;;
  c20|C20) run_c20 ;;
  c21|C21) run_c21 ;;
  c22|C22) run_c22 ;;
  ALL|all)
    run_c1; run_c2; run_c3; run_c4; run_c5; run_c6; run_c7
    run_c8; run_c9; run_c10; run_c11; run_c12; run_c13; run_c14; run_c15
    run_c16; run_c17; run_c18; run_c19; run_c20; run_c21; run_c22
    ;;
  *)
    echo "usage: $0 [c1|C1|...|c22|C22|ALL]"
    exit 1
    ;;
esac

echo ""
echo "==== E2E 结果：PASS=$PASS FAIL=$FAIL ===="
echo "提示：测试资源已保留（deployment/rs/pods），可用 kubectl -n flowtest get all 查看；"
echo "      taint 已移除。如需复现 flow-c3 的 Pending 场景，可手动执行："
echo "      kubectl taint nodes $NODE flowtest=block:NoSchedule"
if [ "$FAIL" -gt 0 ]; then
  printf '失败用例: %s\n' "${FAILED_CASES[@]}"
  exit 1
fi
exit 0
