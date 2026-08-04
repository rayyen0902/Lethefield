"""时钟偏移监控告警（红线 6）。

设计依据：设计文档 §11.5 红线 6——节点时钟同步是硬性前提。
时钟跳变会让 Cassandra LWW 写入被"未来时间戳"的旧单元格静默吞掉、
ID 分配挂起（spike 虚拟化环境实测快 68 分钟后回跳）。M1 部署清单要求：
NTP 硬化 + 偏移监控告警，缺失视为未完成。

本模块是告警探测侧：采集各组件时钟、与参考时钟比对、超阈值告警。
"系统看起来在正常运行"不构成时钟正常的证据（与 Dead Man's Switch 同源），
必须主动探测。
"""

from lethefield_clock_monitor.check import OffsetSample, check_offsets, collect_all

__all__ = ["OffsetSample", "check_offsets", "collect_all"]
