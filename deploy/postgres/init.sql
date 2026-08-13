-- M0 最小实现的两张表：决策留痕表单（§11.3）与训练数据授权注册表（§12.4）。
-- 两者都是后续服务的依赖，从 M0 第一天可用（空表即视为可用）。

CREATE TABLE IF NOT EXISTS decision_log (
    id          BIGSERIAL PRIMARY KEY,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    title       TEXT NOT NULL,
    context     TEXT NOT NULL DEFAULT '',
    decision    TEXT NOT NULL,
    rationale   TEXT NOT NULL DEFAULT '',
    decided_by  TEXT NOT NULL,
    -- v1.2 定案三列（M0 任务 5 补齐 §11.3 既定要求，M11 入料口 ① 的前提）：
    -- Agent 建议内容 / 人类处置结果 / §11.2 升级四类（可空）。表单即标注界面。
    agent_suggestion TEXT NOT NULL DEFAULT '',
    outcome     TEXT NOT NULL DEFAULT 'accepted'
                CHECK (outcome IN ('accepted', 'modified', 'rejected')),
    escalation_type TEXT
                CHECK (escalation_type IS NULL OR escalation_type IN
                       ('ex_write_path', 'cross_space', 'novel_error', 'low_confidence'))
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

-- M16 IS 简版三表（v1.2 修订记录第 24 条⑤；既有卷走 migrations/002_is.sql）。
CREATE TABLE IF NOT EXISTS is_accounts (
    account_id  TEXT PRIMARY KEY,
    display_name TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'disabled')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS is_space_owners (
    account_id  TEXT NOT NULL REFERENCES is_accounts (account_id),
    space_id    TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, space_id)
);

CREATE TABLE IF NOT EXISTS is_credentials (
    jti         TEXT PRIMARY KEY,
    account_id  TEXT NOT NULL REFERENCES is_accounts (account_id),
    space_ids   TEXT[] NOT NULL,
    agent_actor_id TEXT NOT NULL,
    scopes      TEXT[] NOT NULL,
    internal    BOOLEAN NOT NULL DEFAULT false,
    status      TEXT NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'revoked')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL,
    revoked_at  TIMESTAMPTZ
);
