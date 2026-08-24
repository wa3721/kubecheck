#!/usr/bin/env bash
# flow 工具 E2E 测试脚本（真实 k8s 集群）
#
# 用法：bash e2e-test.sh [C1|C2|...|C13|ALL]   （默认 ALL）
#
# 前置：kubectl 可用、/tmp/flowbin 已构建、namespace flowtest 存在
#
# 注意：用例结束后保留测试资源（deployment/rs/pods）不清理，供手动测试。
# taint 不保留：它是全局的，残留会让所有用例的 Pod 无法调度（c1/c4/c5 会
# 被污染）。flow-c3 的 Pod 在 taint 移除后会被调度为 Running；如需复现
# Pending 场景，可手动执行：kubectl taint nodes <node> flowtest=block:NoSchedule
#
# 覆盖用例：
#   C1 正常通过（阶段1+2 通过、日志无错误）                 -> exit 0
#   C2 阶段1 Deployment 滚动就绪超时                       -> exit 2
#   C3 阶段2 Pod 未进入 Running 超时                       -> exit 3
#   C4 阶段3 容器异常退出（退出码 != 0）                   -> exit 3
#   C5 阶段3 日志命中错误关键字但 Pod 存活                 -> exit 0（warning）
#   C6 中断信号（SIGINT）                                  -> exit 130
#   C7 -A 监听模式：deployment 更新自动触发检查，中断 watcher -> exit 130
#   C8 日志命中错误关键字但被 ignore 忽略 -> 不落盘          -> exit 0
#   C9 多容器：sidecar 先就绪后报错退出 -> 落盘             -> exit 3
#   C10 重启次数超限（--max-restart 触发）                  -> exit 3
#   C11 多副本聚合（2 副本均先就绪后报错退出）              -> exit 3
#   C12 无错误关键字（仅 INFO/WARN）-> 缓冲后丢弃不落盘     -> exit 0
#   C13 命中错误 + ignore 混合 -> 落盘仅含非 ignore 错误行  -> exit 0
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
  local name=$1; shift
  timeout 300 "$BIN" deployment/"$name" -n "$NS" \
    --log-error-dir "$LOGDIR" -v "$@" 2>&1 | tee "$LOGDIR/$name.out" >&2
  echo ${PIPESTATUS[0]}
}

# 测试资源（deployment/rs/pods）保留不清理；taint 是全局的，退出时统一移除，
# 避免残留污染其他用例 / 用户后续手动测试。重复运行同一用例时，
# 用例开头的 cleanup_deploy 会先重建同名资源。
trap remove_taint EXIT TERM INT

# ---------- 用例 ----------
run_c1() { # 正常通过 -> exit 0
  local C=c1
  echo "---- [$C] 正常通过 ----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=nginx:1.25 -n "$NS" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --deploy-ready-timeout 60 --pod-ready-timeout 30 --log-check-timeout 10)
  check_result "flow-$C" "$code" 0 "全部 Pod 运行正常"
  # 保留测试资源（deployment/rs/pods）供手动测试
}

run_c2() { # 阶段1 滚动就绪超时 -> exit 2
  local C=c2
  echo "---- [$C] 阶段1 滚动就绪超时 ----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=busybox:1.36 -n "$NS" -- /bin/sh -c "sleep 3600" >/dev/null 2>&1
  # exec 探针永远失败 -> Pod 永不 Ready -> 滚动永不完成（阶段1 条件永不满足）
  kubectl -n "$NS" patch deployment "flow-$C" -p \
    '{"spec":{"template":{"spec":{"containers":[{"name":"busybox","readinessProbe":{"exec":{"command":["/bin/false"]},"periodSeconds":2,"failureThreshold":3}}]}}}}' >/dev/null 2>&1
  # 等 Pod 进入 Running（但 Ready=0/1）
  kubectl -n "$NS" wait --for=jsonpath='{.status.phase}'=Running pod -l "app=flow-$C" --timeout=60s >/dev/null 2>&1
  sleep 3
  local code
  code=$(run_tool "flow-$C" --deploy-ready-timeout 15 --pod-ready-timeout 30 --log-check-timeout 5)
  check_result "flow-$C" "$code" 2 "滚动更新未完成"
  # 保留测试资源（deployment/rs/pods）供手动测试
}

run_c3() { # 阶段2 Pod 未进入 Running 超时 -> exit 3
  local C=c3
  echo "---- [$C] 阶段2 Pod 未进入 Running 超时 ----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=busybox:1.36 -n "$NS" -- /bin/sh -c "sleep 3600" >/dev/null 2>&1
  # replicas=0：阶段1 条件 0==0 && 0==0 立即通过，随后阶段2 等待 Pod 出现
  kubectl -n "$NS" scale deployment "flow-$C" --replicas=0 >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=60s >/dev/null 2>&1
  apply_taint
  # 启动工具（阶段1 秒过），2s 后 scale 出 2 个被 taint 阻挡的 Pending Pod
  (timeout 300 "$BIN" deployment/"flow-$C" -n "$NS" \
    --log-error-dir "$LOGDIR" -v \
    --deploy-ready-timeout 30 --pod-ready-timeout 15 --log-check-timeout 5 \
    2>&1 | tee "$LOGDIR/flow-$C.out"; echo ${PIPESTATUS[0]} >"$LOGDIR/flow-$C.code") &
  sleep 2
  kubectl -n "$NS" scale deployment "flow-$C" --replicas=2 >/dev/null 2>&1
  wait
  local code
  code=$(cat "$LOGDIR/flow-$C.code")
  check_result "flow-$C" "$code" 3 "未进入 running"
  remove_taint
  # 保留 deployment/rs/pods 供手动测试（taint 移除后 Pod 会被调度为 Running）
}

run_c4() { # 阶段3 容器异常退出 -> exit 3
  local C=c4
  echo "---- [$C] 阶段3 容器异常退出 ----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=busybox:1.36 -n "$NS" -- /bin/sh -c "sleep 45; exit 1" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=90s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --deploy-ready-timeout 600 --pod-ready-timeout 30 --log-check-timeout 120)
  check_result "flow-$C" "$code" 3 "容器异常退出"
  # 保留测试资源（deployment/rs/pods）供手动测试
}

run_c5() { # 阶段3 日志报错但存活 -> exit 0 (warning)
  local C=c5
  echo "---- [$C] 阶段3 日志报错但 Pod 存活 ----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=busybox:1.36 -n "$NS" -- /bin/sh -c "while true; do echo 'ERROR something went wrong'; sleep 2; done" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=90s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --deploy-ready-timeout 60 --pod-ready-timeout 30 --log-check-timeout 15)
  check_result "flow-$C" "$code" 0 "日志需检查"
  # 保留测试资源（deployment/rs/pods）供手动测试
}

run_c6() { # 中断信号 -> exit 130
  local C=c6
  echo "---- [$C] 中断信号（SIGINT）----"
  cleanup_deploy "flow-$C"
  kubectl create deployment "flow-$C" --image=busybox:1.36 -n "$NS" -- /bin/sh -c "sleep 3600" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=90s >/dev/null 2>&1
  # 工具直接后台运行，$! 即工具 PID，便于向其发送 SIGINT；
  # 用 process substitution 实时打印并落盘，避免管道使 $! 变成 tee 的 PID
  "$BIN" deployment/"flow-$C" -n "$NS" \
    --log-error-dir "$LOGDIR" -v \
    --deploy-ready-timeout 60 --pod-ready-timeout 30 --log-check-timeout 300 \
    > >(tee "$LOGDIR/flow-$C.out") 2>&1 &
  local tool_pid=$!
  sleep 5
  kill -INT "$tool_pid" 2>/dev/null
  wait "$tool_pid"
  local code=$?
  check_result "flow-$C" "$code" 130 "检查被中断"
  # 保留测试资源（deployment/rs/pods）供手动测试
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
    --log-error-dir "$LOGDIR" -v \
    --deploy-ready-timeout 60 --pod-ready-timeout 30 --log-check-timeout 10 \
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
  # 保留测试资源（deployment/rs/pods）供手动测试
}

run_c8() { # 日志命中错误关键字但被 ignore 忽略 -> 不落盘，exit 0
  local C=c8
  echo "---- [$C] 忽略关键字优先（ERROR 被 ignore）----"
  cleanup_deploy "flow-$C"
  kubectl apply -f "$TESTDIR/flow-$C.yaml" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --deploy-ready-timeout 60 --pod-ready-timeout 30 --log-check-timeout 12 \
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
  # 保留测试资源（deployment/rs/pods）供手动测试
}

run_c9() { # 多容器：sidecar 先就绪后报错退出 -> 落盘，exit 3
  local C=c9
  echo "---- [$C] 多容器（sidecar 先就绪后报错退出）----"
  cleanup_deploy "flow-$C"
  kubectl apply -f "$TESTDIR/flow-$C.yaml" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --deploy-ready-timeout 90 --pod-ready-timeout 30 --log-check-timeout 90)
  check_result "flow-$C" "$code" 3 "容器异常退出"
  # 附加断言：sidecar 的 ERROR 行已落盘
  if ! ls "$LOGDIR"/flowtest-flow-$C-*.log >/dev/null 2>&1; then
    echo "[FAIL] flow-$C: 容器异常退出应产生日志文件"
    FAIL=$((FAIL+1)); FAILED_CASES+=("flow-$C(应落盘但未落盘)")
  else
    echo "[PASS] flow-$C: 已产生日志文件（多容器落盘）"
    PASS=$((PASS+1))
  fi
  # 保留测试资源（deployment/rs/pods）供手动测试
}

run_c10() { # 重启次数超限（--max-restart）-> exit 3
  local C=c10
  echo "---- [$C] 重启次数超限（MaxRestart）----"
  cleanup_deploy "flow-$C"
  kubectl apply -f "$TESTDIR/flow-$C.yaml" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --deploy-ready-timeout 90 --pod-ready-timeout 30 --log-check-timeout 120 \
    --max-restart 1)
  check_result "flow-$C" "$code" 3 "重启次数超过上限"
  # 保留测试资源（deployment/rs/pods）供手动测试
}

run_c11() { # 多副本聚合：2 副本均先就绪后报错退出 -> exit 3
  local C=c11
  echo "---- [$C] 多副本聚合（2 副本均报错退出）----"
  cleanup_deploy "flow-$C"
  kubectl apply -f "$TESTDIR/flow-$C.yaml" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --deploy-ready-timeout 90 --pod-ready-timeout 30 --log-check-timeout 90)
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
  # 保留测试资源（deployment/rs/pods）供手动测试
}

run_c12() { # 无错误关键字（仅 INFO/WARN）-> 缓冲后丢弃不落盘，exit 0
  local C=c12
  echo "---- [$C] 无错误关键字（仅 INFO/WARN）----"
  cleanup_deploy "flow-$C"
  kubectl apply -f "$TESTDIR/flow-$C.yaml" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --deploy-ready-timeout 60 --pod-ready-timeout 30 --log-check-timeout 12 \
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
  # 保留测试资源（deployment/rs/pods）供手动测试
}

run_c13() { # 命中错误 + ignore 混合 -> 落盘仅含非 ignore 错误行，exit 0
  local C=c13
  echo "---- [$C] 命中错误 + ignore 混合 ----"
  cleanup_deploy "flow-$C"
  kubectl apply -f "$TESTDIR/flow-$C.yaml" >/dev/null 2>&1
  kubectl -n "$NS" rollout status deployment/"flow-$C" --timeout=120s >/dev/null 2>&1
  local code
  code=$(run_tool "flow-$C" --deploy-ready-timeout 60 --pod-ready-timeout 30 --log-check-timeout 12 \
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
  # 保留测试资源（deployment/rs/pods）供手动测试
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
  ALL|all)
    run_c1; run_c2; run_c3; run_c4; run_c5; run_c6; run_c7
    run_c8; run_c9; run_c10; run_c11; run_c12; run_c13
    ;;
  *)
    echo "usage: $0 [c1|C1|...|c13|C13|ALL]"
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
