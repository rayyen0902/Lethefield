-- M11：decision_log 补齐 §11.3 既定三列（v1.2 定案，M0 任务 5）。
-- 幂等：对已建卷可重复执行（init.sql 只在新卷初始化时跑，既有卷走本迁移）。
-- 执行：docker compose exec -T postgres psql -U lethefield -d lethefield -f /path/to/001_decision_log_m11.sql

ALTER TABLE decision_log
    ADD COLUMN IF NOT EXISTS agent_suggestion TEXT NOT NULL DEFAULT '',
    ADD COLUMN IF NOT EXISTS outcome TEXT NOT NULL DEFAULT 'accepted',
    ADD COLUMN IF NOT EXISTS escalation_type TEXT;

-- CHECK 约束幂等：Postgres 无 ADD CONSTRAINT IF NOT EXISTS，先删再加。
ALTER TABLE decision_log DROP CONSTRAINT IF EXISTS decision_log_outcome_check;
ALTER TABLE decision_log
    ADD CONSTRAINT decision_log_outcome_check
    CHECK (outcome IN ('accepted', 'modified', 'rejected'));

ALTER TABLE decision_log DROP CONSTRAINT IF EXISTS decision_log_escalation_type_check;
ALTER TABLE decision_log
    ADD CONSTRAINT decision_log_escalation_type_check
    CHECK (escalation_type IS NULL OR escalation_type IN
           ('ex_write_path', 'cross_space', 'novel_error', 'low_confidence'));
