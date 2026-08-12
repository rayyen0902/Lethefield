"""六维打分 prompt 与响应解析（M14）——prompt 模板与解析纪律的单点。

六维语义（设计文档 §1 SS 行：情绪/新颖性/目标/冲突/重要性/显式请求）：
- er（emotional response）：情绪反应强度
- e（explicit request）：用户显式要求记住的程度
- i（importance）：对用户的事实性重要程度
- g（goal）：与用户当前目标/任务的相关性
- n（novelty）：新颖性/意外程度
- c（conflict）：与既有信息冲突或纠正既有信息的程度

解析纪律（v1.2 定案，禁隐式兜底）：整体不可解析 → ValueError（走失败路径）；
单维缺失/非数值/越界 → 计入 missing 清单（走降级规则，由 scoring 模块分级）。
"""

import json
import re

from lethefield_clients.ex_stream import DIMENSIONS

SYSTEM_PROMPT = """你是记忆系统的显著性打分器。对用户的一条记忆事件，按六个维度打 0 到 1 的分值：

- er：情绪反应强度（事件携带的情绪色彩有多强）
- e：显式请求（用户是否显式要求记住这条信息）
- i：重要性（这条信息对用户的事实性重要程度）
- g：目标相关（与用户当前目标、任务、项目的相关程度）
- n：新颖性（相对日常闲聊，这条信息有多意外/稀有）
- c：冲突（这条信息纠正或推翻既有信息的程度）

只输出一个 JSON 对象，键为 "er"、"e"、"i"、"g"、"n"、"c"，值为 0 到 1 的数字。
不要输出任何其他内容。"""


def user_prompt(content: str) -> str:
    """单事件打分的 user 消息（事件原文只进 prompt，不进任何日志/指标）。"""
    return f"记忆事件：\n{content}"


_JSON_BLOCK_RE = re.compile(r"\{[^{}]*\}", re.DOTALL)


def parse_scores(raw: str) -> tuple[dict[str, float], list[str]]:
    """解析 LLM 响应 → (有效维度分值, 缺失维度清单)。

    - 整体不可解析（无 JSON 对象）→ 抛 ValueError（不可解析 = 打分失败路径）；
    - 单维缺失/非数值/越界 [0,1] → 计入 missing（降级规则的作用面）。
    """
    text = raw.strip()
    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        match = _JSON_BLOCK_RE.search(text)
        if match is None:
            raise ValueError("LLM 响应无 JSON 对象（不可解析）") from None
        try:
            obj = json.loads(match.group(0))
        except json.JSONDecodeError:
            raise ValueError("LLM 响应 JSON 块不可解析") from None
    if not isinstance(obj, dict):
        raise ValueError(f"LLM 响应不是 JSON 对象：{type(obj).__name__}")
    dims: dict[str, float] = {}
    missing: list[str] = []
    for dim in DIMENSIONS:
        value = obj.get(dim)
        try:
            value = float(value)
        except (TypeError, ValueError):
            value = None
        if value is None or not 0.0 <= value <= 1.0:
            missing.append(dim)
        else:
            dims[dim] = value
    return dims, missing
