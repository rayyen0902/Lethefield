"""训练数据授权注册表（§12.4 最小实现）。

授权注册表是 M11 授权拦截的前提：加工 worker 每批次必查，
未授权 space 的第 ③④ 类数据在入 topic 前被拒。

注意：`space_ref` 为不透明哈希，不存 space_id 明文（§12.4 样本 schema 同一约定）。
授权范围粒度对应入料口：CALIBRATION = ③ 检索质量/FF 标定明细，
CONTENT_COPY = ④ 用户记忆内容副本。
"""

from lethefield_auth_registry.store import AuthEntry, AuthRegistryStore, AuthScope, AuthStatus

__all__ = ["AuthEntry", "AuthRegistryStore", "AuthScope", "AuthStatus"]
