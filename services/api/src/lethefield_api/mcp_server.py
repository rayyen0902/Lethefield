"""MCP 适配层：四个工具 = service 层的薄壳（stdio 传输）。

业务逻辑全部在 service.py——本文件只做协议翻译；
未来按需求换 MCP 框架或手搓 JSON-RPC 都不动核心（本轮定案备注留痕）。
凭证从环境变量 LETHEFIELD_MCP_TOKEN 读（MCP 是本地进程，token 随环境配置）。

MCP 说明书（引导 LLM 主动使用 Lethefield 的 prompt 工程）按开发文档批注后期独立做。
"""

import os

from mcp.server.fastmcp import FastMCP

from lethefield_api import service
from lethefield_api.auth import verify_token
from lethefield_api.service import ApiContext


def create_mcp_server(ctx: ApiContext) -> FastMCP:
    mcp = FastMCP("lethefield")

    def claims():
        # 每次调用重新验签：token 过期/轮换后自然失效，不缓存 claims
        return verify_token(os.environ.get("LETHEFIELD_MCP_TOKEN", ""))

    @mcp.tool()
    def memory_record(space_id: str, content: str, tau_ms: int | None = None) -> dict:
        """写入新记忆（经 EX 摄入，同步等落库确认）。"""
        return service.record(ctx, claims(), space_id=space_id, content=content, tau_ms=tau_ms)

    @mcp.tool()
    def memory_flag_conflict(
        space_id: str, content: str, ref_conflict: str, tau_ms: int | None = None
    ) -> dict:
        """提交纠错：content 为正确内容，ref_conflict 为被纠正节点 node_key。"""
        return service.flag_conflict(
            ctx,
            claims(),
            space_id=space_id,
            content=content,
            ref_conflict=ref_conflict,
            tau_ms=tau_ms,
        )

    @mcp.tool()
    def memory_reinforce(space_id: str, node_key: str) -> dict:
        """强化已有记忆（+0.2，同步生效）。"""
        return service.reinforce(ctx, claims(), space_id=space_id, node_key=node_key)

    @mcp.tool()
    def memory_retrieve(
        space_id: str,
        query_text: str | None = None,
        query_vector: list[float] | None = None,
        rho: float = 1.0,
        trace_history: bool = False,
    ) -> dict:
        """检索记忆（四阶段，只读）。"""
        return service.retrieve(
            ctx,
            claims(),
            space_id=space_id,
            query_text=query_text,
            query_vector=query_vector,
            rho=rho,
            trace_history=trace_history,
        )

    return mcp


def main() -> None:
    ctx = ApiContext.from_env()
    create_mcp_server(ctx).run()  # stdio


if __name__ == "__main__":
    main()
