#!/usr/bin/env python3
"""端到端演示：用标准库（无需 requests）走完 提交 -> 乱序 -> 缺口 -> 封存 -> 校验。

用法：
    python examples/demo.py                 # 默认 http://localhost:8000
    BASE_URL=http://other:8000 python examples/demo.py

脚本每次运行使用新的随机 source_id，可重复执行而不冲突。
"""

import json
import os
import urllib.error
import urllib.request
import uuid

BASE_URL = os.getenv("BASE_URL", "http://localhost:8000")
GENESIS = "0" * 64


def call(method: str, path: str, body=None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        BASE_URL + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


def show(title, status, body):
    print(f"\n=== {title} -> HTTP {status}")
    print(json.dumps(body, ensure_ascii=False, indent=2)[:1600])


def main():
    source = "demo-" + uuid.uuid4().hex[:8]
    print(f"BASE_URL = {BASE_URL}\nsource_id = {source}")

    # 1. 健康检查
    show("健康检查", *call("GET", "/health"))

    # 2. 按链提交批次 0/1
    prev = GENESIS
    hashes = []
    for n in range(2):
        status, body = call(
            "POST",
            "/batches",
            {
                "source_id": source,
                "batch_no": n,
                "prev_hash": prev,
                "payload": {"event": f"evt-{n}", "n": n},
            },
        )
        show(f"提交批次 {n}", status, body)
        prev = body["batch_hash"]
        hashes.append(prev)

    # 3. 幂等重试批次 1（200 + duplicate=true）
    status, body = call(
        "POST",
        "/batches",
        {
            "source_id": source,
            "batch_no": 1,
            "prev_hash": hashes[0],
            "payload": {"event": "evt-1", "n": 1},
        },
    )
    show("幂等重试批次 1", status, body)

    # 4. 乱序暂存批次 3（缺口为 2）
    status, body = call(
        "POST",
        "/batches",
        {
            "source_id": source,
            "batch_no": 3,
            "prev_hash": "9" * 64,
            "payload": {"event": "evt-3"},
        },
    )
    show("乱序提交批次 3（暂存）", status, body)

    # 5. 查询缺口
    status, gaps = call("GET", f"/sources/{source}/gaps")
    show("查询缺口（缺批次 2）", status, gaps)

    # 6. 尝试封存到 3 -> 409 GAP_NOT_CLOSED
    status, body = call("POST", "/seals", {"source_id": source, "sealed_through": 3})
    show("缺口未闭合时封存（预期 409）", status, body)

    # 7. 补齐批次 2：与批次 1 链接正确，但后到的批次 3 记录的 prev_hash 是假的
    #    -> 入库暂存并返回 422 CHAIN_BROKEN（问题指向后继批次 3）
    status, b2 = call(
        "POST",
        "/batches",
        {
            "source_id": source,
            "batch_no": 2,
            "prev_hash": hashes[1],
            "payload": {"event": "evt-2"},
        },
    )
    show("补齐批次 2（后继 3 链接不符，预期 422）", status, b2)

    # 8. 但批次 3 的 prev_hash 是假的 -> 链在 3 断裂；只能封存到 2
    status, gaps = call("GET", f"/sources/{source}/gaps")
    show("缺口查询（链在批次 3 断裂）", status, gaps)

    # 9. 默认封存到连续链头 2
    status, seal = call("POST", "/seals", {"source_id": source})
    show("封存 0..2", status, seal)
    root = seal["merkle_root"]

    # 10. 再封一次 -> 幂等返回已封存区间到 2
    status, seal2 = call("POST", "/seals", {"source_id": source, "sealed_through": 2})
    show(
        "重复封存（幂等）",
        status,
        {"seal_id": seal2["seal_id"], "already_sealed": seal2["already_sealed"]},
    )

    # 11. 取批次 1 的证明
    status, proof = call("GET", f"/sources/{source}/batches/1/proof")
    show("批次 1 的包含证明", status, proof)

    # 12. 离线校验
    status, v = call(
        "POST",
        "/verify",
        {
            "batch_hash": proof["batch_hash"],
            "merkle_root": root,
            "proof_path": proof["proof_path"],
        },
    )
    show("离线校验证明", status, v)
    assert v["valid"]

    # 13. 查库校验
    status, v = call("POST", "/verify", {"source_id": source, "batch_no": 1})
    show("服务端查库校验", status, v)
    assert v["valid"]

    print("\n演示完成：连续链封存与 Merkle 证明校验全部通过 ✔")


if __name__ == "__main__":
    main()
