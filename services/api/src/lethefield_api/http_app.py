"""FastAPI 装配层：四端点 + Bearer 鉴权 + 限流中间件挂载点。

本层只做协议翻译与横切装配，业务逻辑全部在 service.py（换框架/手搓不动核心）。
端点是 sync def（非 async）：FastAPI 把 sync 端点放到线程池执行——gremlin_python
同步客户端内部 run_until_complete，在 async 端点的事件循环里会直接冲突报错。
"""

from collections.abc import Callable
from typing import Annotated, Protocol

from fastapi import Body, FastAPI, Request, Response
from fastapi.responses import JSONResponse
from lethefield_clients.credentials import CredentialStore
from lethefield_rms.quota import QuotaExceeded
from prometheus_client import CONTENT_TYPE_LATEST, REGISTRY, generate_latest

from lethefield_api import service
from lethefield_api.auth import Claims, reject_actor_spoof, verify_token
from lethefield_api.errors import ApiError, ErrorCode
from lethefield_api.service import ApiContext


class RateLimiter(Protocol):
    """限流中间件挂载点（M5 只定挂载点，阈值标定留待 M12/标定流程）。"""

    def allow(self, claims: Claims, operation: str) -> bool: ...


class NoopRateLimiter:
    def allow(self, claims: Claims, operation: str) -> bool:
        return True


def _required(body: dict, field: str):
    value = body.get(field)
    if value is None:
        raise ApiError(ErrorCode.BAD_REQUEST, f"缺少必填字段 {field!r}")
    return value


def create_app(
    ctx: ApiContext,
    rate_limiter: RateLimiter | None = None,
    revocation_checker: Callable[[str], bool] | None = None,
) -> FastAPI:
    limiter = rate_limiter or NoopRateLimiter()
    # M16：默认接真实吊销列表（仅当 token 带 jti 时才查库——无 jti 的旧 dev
    # token 不触发 PG 依赖，向后兼容）；测试可注入假 checker。
    checker = revocation_checker if revocation_checker is not None else CredentialStore().is_revoked
    app = FastAPI(title="lethefield-api", version="0.1.0")

    @app.exception_handler(ApiError)
    async def _api_error_handler(_request: Request, exc: ApiError) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.body())

    @app.exception_handler(QuotaExceeded)
    async def _quota_error_handler(_request: Request, exc: QuotaExceeded) -> JSONResponse:
        """红线 2（M13）：配额拒绝 → 429 rate_limited（message 含 quota_exceeded）。

        当前无路径触发（写入链 M15 才接 API），此处为预接线。
        """
        return JSONResponse(
            status_code=429,
            content={"error": {"code": str(ErrorCode.RATE_LIMITED), "message": str(exc)}},
        )

    def _claims(request: Request) -> Claims:
        header = request.headers.get("authorization", "")
        if not header.startswith("Bearer "):
            raise ApiError(ErrorCode.UNAUTHORIZED, "缺少 Bearer 凭证")
        return verify_token(header[len("Bearer ") :], is_revoked=checker)

    def _guard(request: Request, operation: str) -> Claims:
        claims = _claims(request)
        if not limiter.allow(claims, operation):  # 限流挂载点
            raise ApiError(ErrorCode.RATE_LIMITED, "请求被限流")
        return claims

    @app.post("/memory/record")
    def record_ep(request: Request, body: Annotated[dict, Body()]) -> dict:
        claims = _guard(request, "record")
        reject_actor_spoof(body)
        return service.record(
            ctx,
            claims,
            space_id=_required(body, "space_id"),
            content=_required(body, "content"),
            tau_ms=body.get("tau_ms"),
        )

    @app.post("/memory/flag_conflict")
    def flag_conflict_ep(request: Request, body: Annotated[dict, Body()]) -> dict:
        claims = _guard(request, "flag_conflict")
        reject_actor_spoof(body)
        return service.flag_conflict(
            ctx,
            claims,
            space_id=_required(body, "space_id"),
            content=_required(body, "content"),
            ref_conflict=_required(body, "ref_conflict"),
            tau_ms=body.get("tau_ms"),
        )

    @app.post("/memory/reinforce")
    def reinforce_ep(request: Request, body: Annotated[dict, Body()]) -> dict:
        claims = _guard(request, "reinforce")
        reject_actor_spoof(body)
        return service.reinforce(
            ctx,
            claims,
            space_id=_required(body, "space_id"),
            node_key=_required(body, "node_key"),
        )

    @app.post("/memory/retrieve")
    def retrieve_ep(request: Request, body: Annotated[dict, Body()]) -> dict:
        claims = _guard(request, "retrieve")
        reject_actor_spoof(body)
        return service.retrieve(
            ctx,
            claims,
            space_id=_required(body, "space_id"),
            query_text=body.get("query_text"),
            query_vector=body.get("query_vector"),
            rho=body.get("rho", 1.0),
            trace_history=body.get("trace_history", False),
        )

    @app.get("/metrics")
    def metrics_ep() -> Response:
        """Prometheus 暴露口（M12）。运维通道：不挂 _claims/_guard，不进业务凭证
        体系——1.0 单节点内网/localhost 暴露口径（2.0 服务商场景再议鉴权）。"""
        return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)

    return app
