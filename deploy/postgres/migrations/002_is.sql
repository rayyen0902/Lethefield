-- M16：IS 简版三表（v1.2 修订记录第 24 条⑤）。
-- 幂等：对已建卷可重复执行（init.sql 只在新卷初始化时跑，既有卷走本迁移）。
-- 执行：docker compose exec -T postgres psql -U lethefield -d lethefield -f /path/to/002_is.sql

-- 账号：账号 → N 个记忆空间归属关系的锚点（开发文档 §17）。
CREATE TABLE IF NOT EXISTS is_accounts (
    account_id  TEXT PRIMARY KEY,
    display_name TEXT NOT NULL DEFAULT '',
    status      TEXT NOT NULL DEFAULT 'active'
                CHECK (status IN ('active', 'disabled')),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 归属关系：账号 → N space（provision 成功后才写行，无半开通状态）。
CREATE TABLE IF NOT EXISTS is_space_owners (
    account_id  TEXT NOT NULL REFERENCES is_accounts (account_id),
    space_id    TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (account_id, space_id)
);

-- 凭证：jti 吊销列表（API 验证侧逐请求查 status；无 jti 旧 dev token 不查，向后兼容）。
-- internal = 内部签发渠道标记：debug scope 只允许 internal=true 的凭证（修订记录 24③）。
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
