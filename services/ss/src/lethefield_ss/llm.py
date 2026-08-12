"""LLM 打分客户端（M14，v1.2 修订记录第 21 条定案）：OpenAI 兼容 HTTP 直连。

复用 httpx、零 SDK、provider 中立（base_url/api_key/model 全走 env 配置）。
key 纪律：只进 Authorization header——不进日志、不进指标、不进异常消息体。
"""

import time

import httpx

from lethefield_ss.config import SSConfig
from lethefield_ss.prompt import SYSTEM_PROMPT, user_prompt


class ScoringError(Exception):
    """打分失败（LLM 超时/限流/5xx/响应结构异常）——走重试 → DLQ 路径。"""


class LLMScorer:
    """单次六维打分调用（temperature=0 求稳定；有限重试 + 退避，耗尽抛 ScoringError）。"""

    def __init__(self, config: SSConfig, client: httpx.Client | None = None) -> None:
        self._config = config
        self._client = client or httpx.Client(timeout=config.llm_timeout_seconds)

    def score(self, content: str) -> tuple[str, dict[str, int], str]:
        """返回 (响应原文, usage{prompt_tokens, completion_tokens}, model)。

        重试面：超时/连接错误/5xx/429（provider 侧临时态）；4xx 其他 = 请求本身
        有问题（模型 ID 错等），立即失败不重试。
        """
        config = self._config
        url = f"{config.llm_base_url.rstrip('/')}/chat/completions"
        payload = {
            "model": config.llm_model,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt(content)},
            ],
            "temperature": 0,
        }
        headers = {"Authorization": f"Bearer {config.llm_api_key}"}
        last_error = ""
        for attempt in range(config.llm_max_retries + 1):
            try:
                resp = self._client.post(url, json=payload, headers=headers)
            except (httpx.TimeoutException, httpx.TransportError) as exc:
                last_error = f"{type(exc).__name__}"
                if attempt < config.llm_max_retries:
                    time.sleep(config.llm_retry_backoff_seconds)
                    continue
                raise ScoringError(f"LLM 调用失败（{last_error}，重试耗尽）") from None
            if resp.status_code == 200:
                return self._parse_response(resp)
            if resp.status_code == 429 or resp.status_code >= 500:
                last_error = f"HTTP {resp.status_code}"
                if attempt < config.llm_max_retries:
                    time.sleep(config.llm_retry_backoff_seconds)
                    continue
                raise ScoringError(f"LLM 调用失败（{last_error}，重试耗尽）") from None
            # 4xx（非 429）：请求本身有问题，不重试。响应体可能含敏感回显，只带状态码。
            raise ScoringError(f"LLM 调用被拒（HTTP {resp.status_code}，不重试）")
        raise ScoringError(f"LLM 调用失败（{last_error}）")  # 不可达，防御

    @staticmethod
    def _parse_response(resp: httpx.Response) -> tuple[str, dict[str, int], str]:
        try:
            body = resp.json()
            text = body["choices"][0]["message"]["content"]
        except (ValueError, KeyError, IndexError, TypeError) as e:
            raise ScoringError("LLM 响应结构异常（choices/message/content 缺失）") from e
        usage_raw = body.get("usage") or {}
        usage = {
            "prompt_tokens": int(usage_raw.get("prompt_tokens") or 0),
            "completion_tokens": int(usage_raw.get("completion_tokens") or 0),
        }
        return text, usage, str(body.get("model") or "")
