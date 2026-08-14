# 运维 Runbook：ES 快照备份/恢复 v0.1（rms_vectors，修订记录第 25 条配套）

**背景**：M7 全保真档重放的"RMS 全部状态"边界 = 图结构 + 状态场字段；`rms_vectors`
（检索面向量唯一载体，embedding 不可重放）不属重放范围，其灾难恢复 = 本 runbook 的
ES 快照备份/恢复，属运维前提。阶段 B 演练实证：无快照时检索面静默全灭（kNN 与关键词
两路均读 rms_vectors，无报错无部分命中），故快照存在性巡检是必备项。

## 前提（已落地）

- compose `es-graph` 已配 `path.repo: /usr/share/elasticsearch/snapshots` + 命名卷
  `es-graph-snapshots`（本机单节点形态；生产多节点形态需换共享文件系统或 S3 仓库插件）。
- **命名卷首次使用需修权限**（卷目录默认 root 属主，ES 进程写不进）：

  ```bash
  docker compose exec --user root es-graph chown elasticsearch:root /usr/share/elasticsearch/snapshots
  ```

## 注册仓库（幂等）

```bash
curl -X PUT 'localhost:9200/_snapshot/lethefield_backup' -H 'Content-Type: application/json' -d '{
  "type": "fs", "settings": {"location": "/usr/share/elasticsearch/snapshots"}
}'
```

## 备份（建议节奏：每日 + 任何 RMS 销毁/迁移演练前）

```bash
curl -X PUT "localhost:9200/_snapshot/lethefield_backup/snap_$(date +%Y%m%d_%H%M)?wait_for_completion=true" \
  -H 'Content-Type: application/json' -d '{"indices": "rms_vectors"}'
```

注意：`make reset` / `down -v` 会清掉快照卷——快照与数据同生命周期，仅防逻辑误删/
索引损坏，不防整机丢失；生产形态仓库必须外置。

## 恢复（先删/关索引再恢复，ES 不允许恢复到已存在的打开索引）

```bash
curl -X POST 'localhost:9200/rms_vectors/_close'
curl -X POST 'localhost:9200/_snapshot/lethefield_backup/snap_<TS>/_restore?wait_for_completion=true' \
  -H 'Content-Type: application/json' -d '{"indices": "rms_vectors"}'
curl -X POST 'localhost:9200/rms_vectors/_open'
```

恢复后校验：`curl 'localhost:9200/rms_vectors/_count'` 与快照时文档数一致；
抽查 `node_key` 与图顶点关联（图侧 node_key 保真由 M7 重放保证，两者对齐即恢复完整）。

## 巡检（接入 DMS/cron 节奏，退出码 1 = 告警）

```bash
uv run python scripts/check_es_snapshot.py            # 仓库注册 + 最新成功快照龄期 ≤48h
```

巡检项：① 仓库已注册且可写；② 存在 state=SUCCESS 快照；③ 最新快照龄期不超阈值
（默认 48h，与每日备份节奏配套）。快照龄期告警为 observation 级（备份任务断裂≠数据面故障）。
旧快照清理当前为人工（fs 仓库，按名前缀删）；留存量与生命周期策略待种子期标定。
