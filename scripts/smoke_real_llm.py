"""真实外部依赖端到端冒烟驱动（阶段 A 工单，人工可读报告的数据采集器）。

用法（栈 + API + SS/writer worker 已就绪后）：
    uv run python scripts/smoke_real_llm.py write --space S --token-file F \\
        [--other-space OS --other-token-file OF] --state var/smoke/state.json
    uv run python scripts/smoke_real_llm.py wait --space S --expect N
    uv run python scripts/smoke_real_llm.py correct --space S --token-file F --state ...
    uv run python scripts/smoke_real_llm.py query --space S --token-file F \\
        --other-space OS --other-token-file OF --state ... --out var/smoke/raw.json

红线 1 合规：全部数据面访问按 --space 收敛（EX 单 keyspace 读、ES count 走
routing + space_id term 双机制）；无跨 space 枚举、无全局扫描。
key 纪律：embedding/LLM key 只经 OpenAIEmbedder 的 Authorization header，
本脚本不打印不落盘。
"""

import argparse
import json
import re
import sys
import time
import urllib.request
from pathlib import Path

import httpx
from lethefield_clients import es_client, ex_cassandra_cluster, ex_n
from lethefield_rms.schema import SCORING_RESULT_META_TYPE, parse_scoring_details
from lethefield_writer.config import WriterConfig
from lethefield_writer.embedding import OpenAIEmbedder

API = "http://127.0.0.1:8000"

# (tag, content)——覆盖爱好/工作/情绪/人际关系/计划/生活事实六类，含两条待纠错旧事实
EVENTS = [
    ("hobby_trail", "我周末喜欢去西山徒步，尤其喜欢秋天看红叶。"),
    ("hobby_coffee", "最近迷上了手冲咖啡，专门买了一套 V60 滤杯在练习。"),
    ("hobby_cilantro", "我不吃香菜，闻到那个味道就难受。"),
    ("work_startup", "我在做一个记忆系统的创业项目，负责整体架构设计。"),
    ("work_investor", "下周三要和投资人开季度汇报会，得提前准备 DAU 数据。"),
    ("work_stack", "团队里我主要用 Python 和 Go 写后端服务。"),
    ("emo_anxiety", "今天早上特别焦虑，因为服务器半夜宕机了。"),
    ("emo_milestone", "项目完成了一个重要里程碑，心情大好，请全团队喝了奶茶。"),
    ("rel_girlfriend", "我女朋友叫小雨，她是做插画设计的。"),
    ("rel_roommate", "张伟是我大学室友，认识十年了，他现在在阿里做产品经理。"),
    ("rel_mom", "我妈每周日晚上都会打电话问我吃饭没有。"),
    ("plan_japan", "计划十月份去日本旅行，最想去京都看寺庙。"),
    ("plan_marathon", "今年的目标是跑完一次半程马拉松。"),
    ("misc_cat", "我家养了一只英短蓝猫，名字叫煤球。"),
    ("misc_allergy", "我对青霉素过敏，看病的时候得提前告诉医生。"),
    ("misc_movie", "我最喜欢的电影是《星际穿越》，已经看了三遍。"),
    ("home_old", "我目前租住在望京的一居室。"),
    ("car_old", "我的车是白色的大众高尔夫。"),
]

# (旧 tag, 新 tag, 纠错内容)——纠错 = 携带 ref_conflict 的普通经验事件
CORRECTIONS = [
    ("home_old", "home_new", "我已经搬家了，现在住在海淀区中关村附近的两居室。"),
    ("car_old", "car_new", "车换了，现在开的是一辆深灰色的特斯拉 Model 3。"),
]

# 对照 space 的诱饵事件（跨 space 零泄漏抽查用）
DECOYS = [
    ("decoy_meeting", "这个空间记录的是另一个项目的周会纪要。"),
    ("decoy_sport", "这个空间的用户喜欢打篮球和游泳。"),
    ("decoy_bike", "这里讨论的是一辆红色的自行车。"),
]

# (tag, query, 预期, 命中判定关键词)——expect: hit / miss
QUERIES = [
    ("q_outdoor", "我喜欢什么户外运动？", "hit", ["徒步", "西山"]),
    ("q_girlfriend", "我女朋友是做什么工作的？", "hit", ["小雨", "插画"]),
    ("q_travel", "我有什么旅行计划？", "hit", ["日本", "京都"]),
    ("q_irrelevant", "量子计算的最新进展是什么？", "miss", []),
    ("q_home", "我现在住在哪里？", "hit", ["中关村", "海淀"]),
    ("q_car", "我开什么车？", "hit", ["Model 3", "特斯拉"]),
    ("q_allergy", "我有什么药物过敏？", "hit", ["青霉素"]),
]

SS_METRICS_URL = "http://127.0.0.1:9105/metrics"
WRITER_METRICS_URL = "http://127.0.0.1:9106/metrics"


def _client(token: str) -> httpx.Client:
    return httpx.Client(base_url=API, headers={"Authorization": f"Bearer {token}"}, timeout=30.0)


def _read_token(path: str) -> str:
    return Path(path).read_text().strip()


def _load_state(path: str) -> dict:
    p = Path(path)
    return json.loads(p.read_text()) if p.exists() else {"events": [], "corrections": []}


def _save_state(path: str, state: dict) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(json.dumps(state, ensure_ascii=False, indent=2))


def cmd_write(args) -> int:
    state = _load_state(args.state)
    with _client(_read_token(args.token_file)) as c:
        for tag, content in EVENTS:
            resp = c.post("/memory/record", json={"space_id": args.space, "content": content})
            resp.raise_for_status()
            body = resp.json()
            state["events"].append(
                {"tag": tag, "event_id": body["event_id"], "n": body["n"], "content": content}
            )
            print(f"[record] n={body['n']:>2} {tag}")
    if args.other_space and args.other_token_file:
        state["decoys"] = []
        with _client(_read_token(args.other_token_file)) as c:
            for tag, content in DECOYS:
                resp = c.post(
                    "/memory/record", json={"space_id": args.other_space, "content": content}
                )
                resp.raise_for_status()
                body = resp.json()
                state["decoys"].append(
                    {"tag": tag, "event_id": body["event_id"], "n": body["n"], "content": content}
                )
                print(f"[decoy] n={body['n']:>2} {tag} @ {args.other_space}")
    _save_state(args.state, state)
    return 0


def _pipeline_counts(space: str) -> tuple[int, int]:
    """(EX scoring_result 数, rms_vectors 该 space 文档数)——SS/writer 完成度探针。"""
    session = ex_cassandra_cluster().connect()
    try:
        metas = ex_n.list_meta_events(session, space_id=space)
        scored = sum(1 for m in metas if m.meta_type == SCORING_RESULT_META_TYPE)
    finally:
        session.cluster.shutdown()
    es = es_client()
    resp = es.count(
        index="rms_vectors",
        query={"term": {"space_id": space}},
        routing=space,
    )
    return scored, int(resp["count"])


def cmd_wait(args) -> int:
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        try:
            scored, vectored = _pipeline_counts(args.space)
        except Exception as exc:  # 索引未建等瞬态
            print(f"[wait] 探针异常（重试）: {type(exc).__name__}")
            time.sleep(args.interval)
            continue
        print(f"[wait] scoring_result={scored} vectors={vectored} 期望≥{args.expect}")
        if scored >= args.expect and vectored >= args.expect:
            return 0
        time.sleep(args.interval)
    print("[wait] 超时")
    return 1


def cmd_correct(args) -> int:
    state = _load_state(args.state)
    by_tag = {e["tag"]: e for e in state["events"]}
    with _client(_read_token(args.token_file)) as c:
        for old_tag, new_tag, content in CORRECTIONS:
            old = by_tag[old_tag]
            ref = f"ev_{old['event_id']}"
            resp = c.post(
                "/memory/flag_conflict",
                json={"space_id": args.space, "content": content, "ref_conflict": ref},
            )
            resp.raise_for_status()
            body = resp.json()
            state["corrections"].append(
                {
                    "old_tag": old_tag,
                    "new_tag": new_tag,
                    "old_node_key": ref,
                    "old_content": old["content"],
                    "event_id": body["event_id"],
                    "n": body["n"],
                    "content": content,
                }
            )
            print(f"[correct] {old_tag} -> {new_tag} (n={body['n']}, ref={ref[:13]}...)")
    _save_state(args.state, state)
    return 0


def _fetch_metrics(url: str) -> dict[str, float]:
    """抓 Prometheus 文本暴露口，只保留 token/调用计数相关序列。"""
    try:
        with urllib.request.urlopen(url, timeout=5) as resp:
            text = resp.read().decode()
    except OSError:
        return {}
    out: dict[str, float] = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.startswith("lethefield_"):
            continue
        name = re.split(r"[{ ]", line, maxsplit=1)[0]
        if not re.search(r"tokens_total|calls_total|dlq_total|degraded", name):
            continue
        labels = re.search(r"\{([^}]*)\}", line)
        key = name + (f"{{{labels.group(1)}}}" if labels else "")
        try:
            out[key] = float(line.rsplit(" ", 1)[1])
        except (ValueError, IndexError):
            continue
    return out


def cmd_query(args) -> int:
    state = _load_state(args.state)
    embedder = OpenAIEmbedder(WriterConfig.from_env())
    embed_usage = {"prompt_tokens": 0, "total_tokens": 0, "calls": 0}
    result: dict = {"space": args.space, "queries": [], "checks": {}}
    main_contents = {e["content"] for e in state["events"]} | {
        c["content"] for c in state["corrections"]
    }

    with _client(_read_token(args.token_file)) as c:
        for tag, text, expect, keywords in QUERIES:
            vector, usage = embedder.embed(text)
            embed_usage["prompt_tokens"] += usage["prompt_tokens"]
            embed_usage["total_tokens"] += usage["total_tokens"]
            embed_usage["calls"] += 1
            resp = c.post(
                "/memory/retrieve",
                json={"space_id": args.space, "query_text": text, "query_vector": vector},
            )
            resp.raise_for_status()
            body = resp.json()
            joined = " ".join(n["content"] for n in body["nodes"])
            hit = any(k in joined for k in keywords) if keywords else not body["nodes"]
            ok = hit if expect == "hit" else not any(k in joined for k in ["徒步", "小雨"])
            entry = {
                "tag": tag,
                "query": text,
                "expect": expect,
                "judged_ok": ok,
                "nodes": body["nodes"],
                "edges": body["edges"],
            }
            result["queries"].append(entry)
            print(f"[query] {tag}: nodes={len(body['nodes'])} judged_ok={ok}")

        # 鉴权层跨 space：主 space 凭证访问对照 space → 403 forbidden_space
        resp = c.post(
            "/memory/retrieve",
            json={"space_id": args.other_space, "query_text": "我开什么车？"},
        )
        result["checks"]["authz_cross_space"] = {
            "status": resp.status_code,
            "body": resp.json(),
            "ok": resp.status_code == 403,
        }
        print(f"[check] authz_cross_space: {resp.status_code}")

    # 数据层跨 space：对照 space 内检索不应出现主 space 内容
    with _client(_read_token(args.other_token_file)) as c:
        vector, usage = embedder.embed("我开什么车？")
        embed_usage["prompt_tokens"] += usage["prompt_tokens"]
        embed_usage["total_tokens"] += usage["total_tokens"]
        embed_usage["calls"] += 1
        resp = c.post(
            "/memory/retrieve",
            json={
                "space_id": args.other_space,
                "query_text": "我开什么车？",
                "query_vector": vector,
            },
        )
        resp.raise_for_status()
        body = resp.json()
        leaked = [n for n in body["nodes"] if n["content"] in main_contents]
        result["checks"]["data_cross_space"] = {
            "nodes": body["nodes"],
            "leaked": leaked,
            "ok": not leaked,
        }
        print(f"[check] data_cross_space: leaked={len(leaked)}")

    # s 值分布与六维打分样例（EX scoring_result details 全保真来源）
    session = ex_cassandra_cluster().connect()
    try:
        metas = ex_n.list_meta_events(session, space_id=args.space)
    finally:
        session.cluster.shutdown()
    details = [
        parse_scoring_details(m.details)
        for m in metas
        if m.meta_type == SCORING_RESULT_META_TYPE and m.details
    ]
    s_values = sorted(d.s for d in details)
    by_event = {e["event_id"]: e for e in state["events"] + state["corrections"]}
    samples = []
    for d in details:
        ev = by_event.get(d.event_id)
        if ev and (ev.get("tag") or ev.get("new_tag")) in (
            "emo_anxiety",
            "work_investor",
            "misc_movie",
            "home_new",
        ):
            samples.append(
                {
                    "tag": ev.get("tag") or ev.get("new_tag"),
                    "content": ev["content"],
                    "s": d.s,
                    "dims": d.dims,
                    "degraded": d.degraded,
                    "model_version": d.model_version,
                }
            )
    result["scoring"] = {
        "count": len(details),
        "s_min": s_values[0] if s_values else None,
        "s_max": s_values[-1] if s_values else None,
        "s_mean": sum(s_values) / len(s_values) if s_values else None,
        "s_values": s_values,
        "degraded_count": sum(1 for d in details if d.degraded),
        "samples": samples,
    }
    result["metrics"] = {
        "ss": _fetch_metrics(SS_METRICS_URL),
        "writer": _fetch_metrics(WRITER_METRICS_URL),
        "query_embed_usage": embed_usage,
    }
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    Path(args.out).write_text(json.dumps(result, ensure_ascii=False, indent=2))
    print(f"[ok] 原始结果落盘 {args.out}")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="smoke_real_llm", description="真实 LLM 冒烟驱动")
    sub = parser.add_subparsers(dest="command", required=True)

    def _common(p, *, other=False, state=True):
        p.add_argument("--space", required=True)
        if state:
            p.add_argument("--state", required=True)
        p.add_argument("--token-file", default=None)
        if other:
            p.add_argument("--other-space", default=None)
            p.add_argument("--other-token-file", default=None)

    p_write = sub.add_parser("write", help="写入冒烟事件（含对照 space 诱饵）")
    _common(p_write, other=True)

    p_wait = sub.add_parser("wait", help="等 SS 打分 + writer 建点完成")
    p_wait.add_argument("--space", required=True)
    p_wait.add_argument("--expect", type=int, required=True)
    p_wait.add_argument("--timeout", type=float, default=600.0)
    p_wait.add_argument("--interval", type=float, default=10.0)

    p_correct = sub.add_parser("correct", help="提交两条纠错（flag_conflict）")
    _common(p_correct)

    p_query = sub.add_parser("query", help="执行检索查询并采集报告素材")
    _common(p_query, other=True)
    p_query.add_argument("--out", required=True)

    args = parser.parse_args(argv)
    if args.command == "write":
        return cmd_write(args)
    if args.command == "wait":
        return cmd_wait(args)
    if args.command == "correct":
        return cmd_correct(args)
    if args.command == "query":
        return cmd_query(args)
    return 2


if __name__ == "__main__":
    sys.exit(main())
