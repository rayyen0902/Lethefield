#!/usr/bin/env bash
# 本地 CI 全流程：静态检查 → 单元测试 → 起全栈 → 集成测试（spike 四断言基线）。
# 对齐开发文档 M0 验收：clone 后一条命令起全栈、跑通 CI、全绿。
set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> uv sync"
uv sync

echo "==> ruff check & format"
uv run ruff check .
uv run ruff format --check .

echo "==> unit tests (libs + ops)"
uv run pytest libs ops -q

echo "==> docker compose up"
docker compose up -d
bash scripts/wait_for_stack.sh

echo "==> integration tests (q1-q4 baseline + ops)"
uv run pytest tests/integration -q

echo "==> CI OK"
