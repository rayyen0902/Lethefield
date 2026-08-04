#!/usr/bin/env bash
# 起栈后等待各组件就绪（compose 的 healthcheck 之外的主动探测）。
# 用法：bash scripts/wait_for_stack.sh [总超时秒数，默认 420]
set -euo pipefail

DEADLINE=$(( $(date +%s) + ${1:-420} ))
COMPOSE="docker compose"

log() { echo "[wait] $*"; }

until_ready() {
    local name="$1"; shift
    log "waiting for ${name} ..."
    while ! "$@" > /dev/null 2>&1; do
        if (( $(date +%s) > DEADLINE )); then
            echo "[wait] TIMEOUT waiting for ${name}" >&2
            exit 1
        fi
        sleep 3
    done
    log "${name} ready"
}

until_ready "postgres"      $COMPOSE exec -T postgres pg_isready -U lethefield -d lethefield
until_ready "redis"         $COMPOSE exec -T redis redis-cli ping
until_ready "cassandra-cell" $COMPOSE exec -T cassandra-cell cqlsh -e "describe cluster"
until_ready "cassandra-ex"  $COMPOSE exec -T cassandra-ex cqlsh -e "describe cluster"
until_ready "es-graph"      curl -sf http://localhost:9200/_cluster/health
until_ready "es-ops"        curl -sf http://localhost:9201/_cluster/health
until_ready "pulsar"        curl -sf http://localhost:8080/admin/v2/clusters
until_ready "janusgraph"    bash -c "</dev/tcp/localhost/8182"

# gremlin server 端口开放 ≠ 可执行脚本，再做一次真实脚本探测
log "verifying gremlin script execution ..."
uv run python - <<'EOF'
import time
from gremlin_python.driver.client import Client

deadline = time.time() + 120
while True:
    try:
        # 自定义 server yaml 只绑定 ConfigurationManagementGraph，客户端别名指向它
        client = Client("ws://localhost:8182/gremlin", "ConfigurationManagementGraph")
        assert client.submit("1+1").all().result() == [2]
        client.close()
        break
    except Exception:
        if time.time() > deadline:
            raise
        time.sleep(3)
print("[wait] janusgraph gremlin ready")
EOF

log "stack ready"
