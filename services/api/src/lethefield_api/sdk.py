"""Python SDK：四操作的 httpx 客户端薄封装（错误码映射为 ApiError 异常）。"""

import httpx

from lethefield_api.errors import ApiError, ErrorCode


class MemoryClient:
    """Lethefield 记忆接口客户端。

    transport 可注入（如 httpx.ASGITransport）用于不走真实网络的测试/同进程调用。
    """

    def __init__(
        self,
        base_url: str,
        token: str,
        *,
        transport: httpx.BaseTransport | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._http = httpx.Client(
            base_url=base_url,
            headers={"Authorization": f"Bearer {token}"},
            transport=transport,
            timeout=timeout,
        )

    def close(self) -> None:
        self._http.close()

    def _call(self, operation: str, payload: dict) -> dict:
        response = self._http.post(f"/memory/{operation}", json=payload)
        data = response.json()
        if response.status_code != 200:
            error = data.get("error", {})
            raise ApiError(
                ErrorCode(error.get("code", ErrorCode.INTERNAL)),
                error.get("message", f"HTTP {response.status_code}"),
            )
        return data

    def record(self, *, space_id: str, content: str, tau_ms: int | None = None) -> dict:
        payload: dict = {"space_id": space_id, "content": content}
        if tau_ms is not None:
            payload["tau_ms"] = tau_ms
        return self._call("record", payload)

    def flag_conflict(
        self, *, space_id: str, content: str, ref_conflict: str, tau_ms: int | None = None
    ) -> dict:
        payload: dict = {
            "space_id": space_id,
            "content": content,
            "ref_conflict": ref_conflict,
        }
        if tau_ms is not None:
            payload["tau_ms"] = tau_ms
        return self._call("flag_conflict", payload)

    def reinforce(self, *, space_id: str, node_key: str) -> dict:
        return self._call("reinforce", {"space_id": space_id, "node_key": node_key})

    def retrieve(
        self,
        *,
        space_id: str,
        query_text: str | None = None,
        query_vector: list[float] | None = None,
        rho: float = 1.0,
        trace_history: bool = False,
    ) -> dict:
        return self._call(
            "retrieve",
            {
                "space_id": space_id,
                "query_text": query_text,
                "query_vector": query_vector,
                "rho": rho,
                "trace_history": trace_history,
            },
        )
