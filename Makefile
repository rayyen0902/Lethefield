COMPOSE := docker compose
PYTEST := uv run pytest

.PHONY: up down reset test itest lint ci

## 起全栈并等待就绪
up:
	$(COMPOSE) up -d
	bash scripts/wait_for_stack.sh

## 停栈（保留数据卷）
down:
	$(COMPOSE) down

## 清数据卷重起（危险：清空本地全量数据）
reset:
	$(COMPOSE) down -v
	$(COMPOSE) up -d
	bash scripts/wait_for_stack.sh

## 静态检查
lint:
	uv run ruff check .
	uv run ruff format --check .

## 单元测试（不需要全栈）
test:
	$(PYTEST) libs ops services

## 集成测试（需要全栈已就绪）
itest:
	$(PYTEST) tests/integration

## 本地 CI 全流程
ci: lint test up itest
	@echo "CI OK"
