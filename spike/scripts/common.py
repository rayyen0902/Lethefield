"""Spike 公共工具：Gremlin / ES 连接、等待、合成向量。"""
import hashlib
import time

import numpy as np
import requests
from gremlin_python.driver.client import Client

GREMLIN_URL = "ws://localhost:8182/gremlin"
ES_URL = "http://localhost:9200"
DIMS = 384


# ---------- Gremlin ----------

def submit(script, bindings=None, timeout=600):
    """向 Gremlin Server 提交 Groovy 脚本，返回结果列表。"""
    client = Client(GREMLIN_URL, "g")
    try:
        return client.submit(script, bindings=bindings or {}).all().result()
    finally:
        client.close()


def wait_gremlin(timeout=900, interval=5):
    """轮询直到 Gremlin Server 可响应，返回耗时秒数；超时抛异常。每次尝试新建客户端并带结果超时，避免卡死在挂死连接上。"""
    start = time.time()
    last_err = None
    while time.time() - start < timeout:
        client = None
        try:
            client = Client(GREMLIN_URL, "g")
            r = client.submit("1+1").all().result(timeout=10)
            if r and r[0] == 2:
                return time.time() - start
        except Exception as e:  # noqa: BLE001
            last_err = e
        finally:
            try:
                if client:
                    client.close()
            except Exception:  # noqa: BLE001
                pass
        time.sleep(interval)
    raise TimeoutError(f"gremlin not ready in {timeout}s, last error: {last_err}")


# ---------- Elasticsearch ----------

def es(method, path, ok=(200, 201), **kw):
    r = requests.request(method, ES_URL + path, timeout=60, **kw)
    if r.status_code not in ok:
        raise RuntimeError(f"ES {method} {path} -> {r.status_code}: {r.text[:500]}")
    return r.json() if r.text else {}


def wait_es(timeout=600, interval=5):
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = requests.get(ES_URL + "/_cluster/health", timeout=5)
            if r.status_code == 200 and r.json()["status"] in ("green", "yellow"):
                return time.time() - start
        except Exception:  # noqa: BLE001
            pass
        time.sleep(interval)
    raise TimeoutError(f"ES not ready in {timeout}s")


# ---------- 合成向量 ----------

def make_rng(seed_text):
    seed = int(hashlib.sha256(seed_text.encode()).hexdigest(), 16) % (2 ** 32)
    return np.random.default_rng(seed)


def cluster_centers(n=3, dims=DIMS):
    """固定的 n 个单位向量中心（确定性）。"""
    rng = make_rng("lethefield-centers")
    c = rng.normal(size=(n, dims))
    return c / np.linalg.norm(c, axis=1, keepdims=True)


def synth_vector(center, noise, rng):
    v = center + rng.normal(scale=noise, size=center.shape)
    return (v / np.linalg.norm(v)).astype(np.float32)


def cosine(a, b):
    return float(np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b)))
