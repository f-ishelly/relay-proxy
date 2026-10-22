#!/usr/bin/env bash
# 续租压测。默认在本进程拉起 3 节点 etcd，全部客户端配置全部成员。
# 环境变量：HOSTS CONNS KEEP KILL_AFTER
set -euo pipefail
cd "$(dirname "$0")/.."
exec go run ./cmd/loadgen \
  -embed \
  -label "${LABEL:-acceptance7}" \
  -hosts "${HOSTS:-1000}" \
  -conns "${CONNS:-32}" \
  -ttl 10 \
  -renew 3s \
  -kill-after "${KILL_AFTER:-2s}" \
  -keep "${KEEP:-20s}" \
  -register-timeout "${REGISTER_TIMEOUT:-60s}"
