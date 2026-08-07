"""训练数据管线（M11，1.0 最小实现）。

设计依据：开发文档 §12 / 设计文档 §12.3/§12.4。边界：
- 加工 worker 只从训练 topic / 本地热层取数，**不查业务库**（红线 1；
  静态测试强制：worker 侧模块禁止 import gremlin/cassandra/ex_n）。
- 刻意不做全量沉淀：未命中 R1–R3 的流量不产样本；召回明细短 retention 过境
  topic（过境 ≠ 沉淀）。
- 可撤回性 1.0 内建：space_ref 哈希 + 清单索引使定位是 O(清单) 操作。
"""

from lethefield_training.sample import SAMPLE_VERSION, TrainingSample

__all__ = ["SAMPLE_VERSION", "TrainingSample"]
