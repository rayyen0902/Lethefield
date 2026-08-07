"""EX 摄入 Dead Man's Switch（M10）。

设计依据：设计文档 §7.5.1——"系统看起来在正常运行"不构成摄入正常的证据，
必须主动探测。本包是摄入链路的告警探测侧，监控信号与数据面物理分离：

1. 管道活性探针（page 级）：向独立监控 tenant 的 probe topic 发消息并自消费 ack，
   验证 broker → BookKeeper → consumer ack 端到端活性；严禁向任何 space 的
   namespace 发探针（会污染数据面）。
2. 训练控制 topic consumer backlog 监控（page 级）：consumer 停摆 = 销毁指令
   静默积压 = 合规失效（契约 5 硬性约束 2）。
3. space 写入新鲜度（observation 级）：活跃 space 的成功摄入时间超阈值即 stale，
   翻转边留痕（fresh→stale 告警一次，stale→fresh 恢复事件）。

本包只做探测与告警：共享代码只允许 libs/ 三样（服务边界约定），
topic stats 的 admin REST 查询在本包内自实现，不 import 任何 services/*。
"""

from lethefield_ingest_dms.backlog import check_backlog, fetch_training_backlog, report_backlog
from lethefield_ingest_dms.config import DmsConfig
from lethefield_ingest_dms.freshness import check_freshness
from lethefield_ingest_dms.probe import ensure_monitoring_topic, probe_pipeline

__all__ = [
    "DmsConfig",
    "check_backlog",
    "check_freshness",
    "ensure_monitoring_topic",
    "fetch_training_backlog",
    "probe_pipeline",
    "report_backlog",
]
