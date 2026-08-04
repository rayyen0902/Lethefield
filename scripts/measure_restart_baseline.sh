#!/usr/bin/env bash
# M1 验收第 4 条：容器重启至可服务状态的时间基线。
# spike 参考值 ~52s（JanusGraph 单节点含 CMG 初始化），仅作容量规划对比，非硬性指标。
# 手动执行：bash scripts/measure_restart_baseline.sh（不进 CI——基线记录，不是质量门）
set -euo pipefail
cd "$(dirname "$0")/.."

OUT=deploy/baselines/restart_baseline.jsonl
mkdir -p deploy/baselines

measure() {
    local name="$1" probe="$2"
    echo "==> restarting ${name} ..."
    docker compose restart "${name}" > /dev/null
    local start
    start=$(date +%s)
    until bash -c "${probe}" > /dev/null 2>&1; do
        sleep 2
        if (( $(date +%s) - start > 600 )); then
            echo "{\"component\": \"${name}\", \"error\": \"timeout\", \"measured_at\": \"$(date -u +%FT%TZ)\"}" | tee -a "${OUT}"
            return 1
        fi
    done
    local elapsed=$(( $(date +%s) - start ))
    echo "{\"component\": \"${name}\", \"restart_to_ready_seconds\": ${elapsed}, \"measured_at\": \"$(date -u +%FT%TZ)\"}" | tee -a "${OUT}"
}

measure postgres "docker compose exec -T postgres pg_isready -U lethefield -d lethefield"
measure redis "docker compose exec -T redis redis-cli ping"
measure cassandra-cell "docker compose exec -T cassandra-cell cqlsh -e 'describe cluster'"
measure cassandra-ex "docker compose exec -T cassandra-ex cqlsh -e 'describe cluster'"
measure es-graph "curl -sf http://localhost:9200/_cluster/health"
measure es-ops "curl -sf http://localhost:9201/_cluster/health"
measure pulsar "curl -sf http://localhost:8080/admin/v2/clusters"
measure janusgraph "uv run python -c \"
from gremlin_python.driver.client import Client
c = Client('ws://localhost:8182/gremlin', 'ConfigurationManagementGraph')
assert c.submit('1+1').all().result() == [2]
c.close()\""

echo "==> baseline written to ${OUT}"
