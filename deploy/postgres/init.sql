-- M0 最小实现的两张表：决策留痕表单（§11.3）与训练数据授权注册表（§12.4）。
-- 两者都是后续服务的依赖，从 M0 第一天可用（空表即视为可用）。

CREATE TABLE IF NOT EXISTS decision_log (
    id          BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    title       TEXT NOT NULL,
    context     TEXT NOT NULL DEFAULT '',
    decision    TEXT NOT NULL,
    rationale   TEXT NOT NULL DEFAULT '',
    decided_by  TEXT NOT NULL
);

-- space_ref 为不透明哈希，不存 space_id 明文（与 §12.4 样本 schema 同一约定）；
-- scopes 对应入料口粒度：calibration = ③，content_copy = ④。
CREATE TABLE IF NOT EXISTS auth_registry (
    space_ref   TEXT PRIMARY KEY,
    scopes      TEXT[] NOT NULL,
    status      TEXT NOT NULL DEFAULT 'active',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
