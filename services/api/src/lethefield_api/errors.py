"""对外错误码契约（M5 初版冻结，B 侧维护）。

内部失败模式一律映射到这套码；各服务不允许自定义对外错误格式（任务划分隐含契约）。
限流阈值等具体参数留待标定，错误码集合本身是契约。
"""

from enum import StrEnum


class ErrorCode(StrEnum):
    UNAUTHORIZED = "unauthorized"  # 401：token 缺失/无效/过期
    FORBIDDEN_SCOPE = "forbidden_scope"  # 403：凭证无该操作 scope
    FORBIDDEN_SPACE = "forbidden_space"  # 403：凭证不覆盖请求的 space_id
    ACTOR_SPOOF = "actor_spoof"  # 400：请求体试图声明 agent_actor_id
    BAD_REQUEST = "bad_request"  # 400：参数缺失/非法
    NOT_FOUND = "not_found"  # 404：目标资源不存在
    RATE_LIMITED = "rate_limited"  # 429：被限流
    INTERNAL = "internal"  # 500：未映射的内部错误


_STATUS = {
    ErrorCode.UNAUTHORIZED: 401,
    ErrorCode.FORBIDDEN_SCOPE: 403,
    ErrorCode.FORBIDDEN_SPACE: 403,
    ErrorCode.ACTOR_SPOOF: 400,
    ErrorCode.BAD_REQUEST: 400,
    ErrorCode.NOT_FOUND: 404,
    ErrorCode.RATE_LIMITED: 429,
    ErrorCode.INTERNAL: 500,
}


class ApiError(Exception):
    """可映射到对外错误码的异常；http_app/sdk 两侧共用。"""

    def __init__(self, code: ErrorCode, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message

    @property
    def http_status(self) -> int:
        return _STATUS[self.code]

    def body(self) -> dict:
        return {"error": {"code": str(self.code), "message": self.message}}
