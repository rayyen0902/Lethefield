# M12 开发期最小集指标可查询性验证记录（v1）

- 日期：2026-08-15 00:46:01 +0800
- 环境：Mac dev 机（colima docker compose 全栈 + 宿主机 uv run 服务进程）
- Prometheus：http://localhost:9090（scrape 走 host.docker.internal）
- 生成：`uv run python scripts/verify_metrics_queryable.py`（可重跑，覆盖本文件）
- 判定口径：有数据 = 序列非空；无数据 = /api/v1/query status=success（语法合法）。histogram 按名断言查 `<name>_count`（基名只暴露 _bucket/_sum/_count）。类型经 /api/v1/metadata 与源码定义类型比对，target 无数据时记『类型未取得』。

## Prometheus targets 健康

注：lethefield-ss / lethefield-training / lethefield-writer 不在最小集指标来源内（16 项全部由 api/fs/ingest-dms/metrics-exporter 四进程覆盖），本验证不起这三个进程。

| job | health |
|---|---|
| lethefield-api | up |
| lethefield-fs | up |
| lethefield-ingest-dms | up |
| lethefield-metrics-exporter | up |
| lethefield-ss | down |
| lethefield-training | down |
| lethefield-writer | down |

## 逐项断言（15 项 / 16 个指标名）

| 指标名 | 查询 | 类型 | 结果 | 判定 |
|---|---|---|---|---|
| `lethefield_graph_open_duration_seconds` | `lethefield_graph_open_duration_seconds_count` | 类型一致（histogram） | 2 序列：{instance='host.docker.internal:9104',job='lethefield-metrics-exporter',type='warm'}=2; {instance='host.docker.internal:9104',job='lethefield-metrics-exporter',type='cold'}=58 | PASS（有数据） |
| `lethefield_graph_lru_cache_hit_ratio` | `lethefield_graph_lru_cache_hit_ratio` | 类型一致（gauge） | 1 序列：{instance='host.docker.internal:9104',job='lethefield-metrics-exporter'}=0.7333333333333334 | PASS（有数据） |
| `lethefield_retrieve_stage_duration_seconds` | `lethefield_retrieve_stage_duration_seconds_count` | 类型一致（histogram） | 3 序列：{instance='host.docker.internal:8000',job='lethefield-api',stage='knn'}=3; {instance='host.docker.internal:8000',job='lethefield-api',stage='subgraph'}=3; {instance='host.docker.internal:8000',job='lethefield-api',stage='ff_filter'}=3 | PASS（有数据） |
| `lethefield_record_confirm_duration_seconds` | `lethefield_record_confirm_duration_seconds_count` | 类型一致（histogram） | 1 序列：{instance='host.docker.internal:8000',job='lethefield-api'}=3 | PASS（有数据） |
| `lethefield_pulsar_backlog_events` | `lethefield_pulsar_backlog_events` | 类型一致（gauge） | 1 序列：{instance='host.docker.internal:9103',job='lethefield-ingest-dms',namespace_class='training_control'}=0 | PASS（有数据） |
| `lethefield_ex_write_duration_seconds` | `lethefield_ex_write_duration_seconds_count` | 类型一致（histogram） | 1 序列：{instance='host.docker.internal:8000',job='lethefield-api'}=3 | PASS（有数据） |
| `lethefield_ex_last_write_age_seconds` | `lethefield_ex_last_write_age_seconds` | 类型一致（gauge） | 2 序列：{dimension='max',instance='host.docker.internal:9103',job='lethefield-ingest-dms'}=41763.27143; {dimension='p95',instance='host.docker.internal:9103',job='lethefield-ingest-dms'}=41763.27143 | PASS（有数据） |
| `lethefield_fs_sweep_lag_seconds` | `lethefield_fs_sweep_lag_seconds` | 类型一致（gauge） | 1 序列：{instance='host.docker.internal:9101',job='lethefield-fs'}=60.01857304573059 | PASS（有数据） |
| `lethefield_fs_sweep_processed_total` | `lethefield_fs_sweep_processed_total` | 类型未取得 | 4 序列：{instance='host.docker.internal:9101',job='lethefield-fs',result='neglected'}=0; {instance='host.docker.internal:9101',job='lethefield-fs',result='archived'}=0; {instance='host.docker.internal:9101',job='lethefield-fs',result='consolidated'}=0 …共4序列 | PASS（有数据） |
| `lethefield_cell_watermark_ratio` | `lethefield_cell_watermark_ratio` | 类型一致（gauge） | 1 序列：{cell_id='m9cell0b3ecd',dimension='es_shards',instance='host.docker.internal:9104',job='lethefield-metrics-exporter'}=0.95 | PASS（有数据） |
| `lethefield_space_storage_bytes` | `lethefield_space_storage_bytes` | 类型一致（gauge） | 1 序列：{instance='host.docker.internal:9104',job='lethefield-metrics-exporter',tier='cold'}=2479645 | PASS（有数据） |
| `lethefield_ff_theta_filter_ratio` | `lethefield_ff_theta_filter_ratio_count` | 类型一致（histogram） | 1 序列：{instance='host.docker.internal:8000',job='lethefield-api'}=3 | PASS（有数据） |
| `lethefield_ff_delta_applied_total` | `lethefield_ff_delta_applied_total` | 类型未取得 | 1 序列：{instance='host.docker.internal:9104',job='lethefield-metrics-exporter',type='neglect'}=3 | PASS（有数据） |
| `lethefield_ff_recalled_then_touched_rate` | `lethefield_ff_recalled_then_touched_rate` | 类型一致（gauge） | 1 序列：{instance='host.docker.internal:9104',job='lethefield-metrics-exporter'}=0.3333333333333333 | PASS（有数据） |
| `lethefield_agent_suggestion_total` | `lethefield_agent_suggestion_total` | 类型未取得 | 3 序列：{instance='host.docker.internal:9104',job='lethefield-metrics-exporter',outcome='rejected'}=2; {instance='host.docker.internal:9104',job='lethefield-metrics-exporter',outcome='modified'}=1; {instance='host.docker.internal:9104',job='lethefield-metrics-exporter',outcome='accepted'}=1 | PASS（有数据） |
| `lethefield_escalation_total` | `lethefield_escalation_total` | 类型未取得 | 2 序列：{instance='host.docker.internal:9104',job='lethefield-metrics-exporter',reason='cross_space'}=1; {instance='host.docker.internal:9104',job='lethefield-metrics-exporter',reason='novel_error'}=1 | PASS（有数据） |

## Grafana 面板 PromQL（deploy/grafana/provisioning/dashboards/lethefield.json）

| 面板 | expr | 结果 | 判定 |
|---|---|---|---|
| 告警线：record_confirm 延迟 p50 | `histogram_quantile(0.5, sum(rate(lethefield_record_confirm_duration_seconds_bucket[5m])) by (le))` | 1 序列：=NaN | PASS（有数据） |
| 告警线：EX 最后写入年龄 | `lethefield_ex_last_write_age_seconds` | 2 序列：{dimension='max',instance='host.docker.internal:9103',job='lethefield-ingest-dms'}=41763.27143; {dimension='p95',instance='host.docker.internal:9103',job='lethefield-ingest-dms'}=41763.27143 | PASS（有数据） |
| 标定线：θ 过滤比 p50 | `histogram_quantile(0.5, sum(rate(lethefield_ff_theta_filter_ratio_bucket[5m])) by (le))` | 1 序列：=NaN | PASS（有数据） |
| 标定线：δ 应用计数（按 type） | `sum by (type) (rate(lethefield_ff_delta_applied_total[5m]))` | 1 序列：{type='neglect'}=0 | PASS（有数据） |
| 留痕线：agent 建议计数（按 outcome） | `sum by (outcome) (rate(lethefield_agent_suggestion_total[5m]))` | 3 序列：{outcome='rejected'}=0; {outcome='modified'}=0; {outcome='accepted'}=0 | PASS（有数据） |
| 留痕线：升级计数（按 reason） | `sum by (reason) (rate(lethefield_escalation_total[5m]))` | 2 序列：{reason='cross_space'}=0; {reason='novel_error'}=0 | PASS（有数据） |

## 汇总：全部 PASS
