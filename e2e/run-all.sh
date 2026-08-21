#!/usr/bin/env bash
set -u
export PATH=$PATH:/usr/local/go/bin
cd /root/flow/flow
export GOPROXY=https://goproxy.cn,direct
export GOFLAGS=-mod=mod

echo "== go vet =="
go vet ./... 2>&1 || exit 1
echo "== go build =="
go build -o /tmp/flowbin . 2>&1 || exit 1
echo "== go test =="
go test ./... 2>&1 || exit 1
echo "== e2e ALL =="
bash e2e/e2e-test.sh ALL
exit $?
