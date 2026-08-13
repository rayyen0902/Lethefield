"""Embedding 客户端（M15，修订记录第 23 条④）：OpenAI 兼容 /embeddings HTTP 直连。

复用 httpx、零 SDK、provider 中立（base_url/api_key/model/dims 全走 env 配置，
与 M14 SS llm.py 同款形态）。key 纪律：只进 Authorization header——不进日志、
不进指标、不进异常消息体。

一致性规则：返回向量维度必须等于配置 dims（= rms_vectors mapping dims）——
共享索引内混入异模型/异维度向量会破坏检索语义，维度不符按失败处理（EmbedError），
不静默落库。模型变更 = 向量全量重建，进决策留痕。
"""

import time

import httpx

from lethefield_writer.config import WriterConfig


class EmbedError(Exception):
    """嵌入失败（超时/限流/5xx/响应结构异常/维度不符）——走重试 → DLQ 路径。"""


class OpenAIEmbedder:
    """单次文本嵌入调用（有限重试 + 退避，耗尽抛 EmbedError）。"""

    def __init__(self, config: WriterConfig, client: httpx.Client | None = None) -> None:
        self._config = config
        self._client = client or httpx.Client(timeout=config.embed_timeout_seconds)

    def embed(self, text: str) -> tuple[list[float], dict[str, int]]:
        """返回 (向量, usage{prompt_tokens, total_tokens})。

        重试面：超时/连接错误/5xx/429（provider 侧临时态）；4xx 其他 = 请求本身
        有问题（模型 ID 错等），立即失败不重试。
        """
        config = self._config
        url = f"{config.embed_base_url.rstrip('/')}/embeddings"
        payload = {"model": config.embed_model, "input": text}
        headers = {"Authorization": f"Bearer {config.embed_api_key}"}
        last_error = ""
        for attempt in range(config.embed_max_retries + 1):
            try:
                resp = self._client.post(url, json=payload, headers=headers)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = f"{type(exc).__name__}"
                if attempt < config.embed_max_retries:
                    time.sleep(config.embed_retry_backoff_seconds)
                    continue
                raise EmbedError(f"embedding 调用失败（{last_error}，重试耗尽）") from None
            if resp.status_code == 200:
                return self._parse_response(resp)
            if resp.status_code == 429 or resp.status_code >= 500:
                last_error = f"HTTP {resp.status_code}"
                if attempt < config.embed_max_retries:
                    time.sleep(config.embed_retry_backoff_seconds)
                    continue
                raise EmbedError(f"embedding 调用失败（{last_error}，重试耗尽）") from None
            # 4xx（非 429）：请求本身有问题，不重试。响应体可能含敏感回显，只带状态码。
            raise EmbedError(f"embedding 调用被拒（HTTP {resp.status_code}，不重试）")
        raise EmbedError(f"embedding 调用失败（{last_error}）")  # 不可达，防御

    def _parse_response(self, resp: httpx.Response) -> tuple[list[float], dict[str, int]]:
        try:
            body = resp.json()
            vector = [float(x) for x in body["data"][0]["embedding"]]
        except (ValueError, KeyError, IndexError, TypeError) as e:
            raise EmbedError("embedding 响应结构异常（data/embedding 缺失）") from e
        dims = self._config.embed_dims
        if dims > 0 and len(vector) != dims:
            raise EmbedError(
                f"embedding 维度不符：返回 {len(vector)}，期望 {dims}"
                "（共享 rms_vectors 必须同模型同维度，修订记录第 23 条④）"
            )
        usage_raw = body.get("usage") or {}
        usage = {
            "prompt_tokens": int(usage_raw.get("prompt_tokens") or 0),
            "total_tokens": int(usage_raw.get("total_tokens") or 0),
        }
        return vector, usage
