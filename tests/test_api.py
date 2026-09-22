"""端到端接口测试。"""

import concurrent.futures


def _event(n):
    return {"seq": n, "type": "log.ingest", "data": {"line": f"event-{n}", "v": 1}}


def _submit(client, source, n, prev, payload=None):
    return client.post(
        "/batches",
        json={
            "source_id": source,
            "batch_no": n,
            "prev_hash": prev,
            "payload": payload if payload is not None else _event(n),
        },
    )


GENESIS = "0" * 64


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


def test_full_ordered_flow_with_seal_and_verify(client):
    source = "probe-A"

    # 依次提交 0..4
    prev = GENESIS
    hashes = []
    for n in range(5):
        r = _submit(client, source, n, prev)
        assert r.status_code == 201, r.text
        body = r.json()
        assert body["duplicate"] is False
        hashes.append(body["batch_hash"])
        prev = body["batch_hash"]

    gaps = client.get(f"/sources/{source}/gaps").json()
    assert gaps["missing"] == []
    assert gaps["next_expected_batch"] == 0
    assert gaps["contiguous_head"] == 4
    assert gaps["ready_to_seal_through"] == 4
    assert gaps["has_gap"] is False

    # 封存 0..3（封存前缀）
    r = client.post("/seals", json={"source_id": source, "sealed_through": 3})
    assert r.status_code == 200, r.text
    seal = r.json()
    assert seal["sealed_from"] == 0
    assert seal["sealed_through"] == 3
    assert seal["leaf_count"] == 4
    assert len(seal["proofs"]) == 4
    root = seal["merkle_root"]
    seal_id = seal["seal_id"]

    # 幂等：再次封存同一区间
    r2 = client.post("/seals", json={"source_id": source, "sealed_through": 3})
    assert r2.status_code == 200
    body2 = r2.json()
    assert body2["already_sealed"] is True
    assert body2["merkle_root"] == root
    assert body2["seal_id"] == seal_id

    # 取批次 2 的证明并离线校验
    r = client.get(f"/sources/{source}/batches/2/proof")
    assert r.status_code == 200
    proof = r.json()
    assert proof["index"] == 2
    assert proof["merkle_root"] == root
    assert proof["batch_hash"] == hashes[2]

    r = client.post(
        "/verify",
        json={
            "batch_hash": hashes[2],
            "merkle_root": root,
            "proof_path": proof["proof_path"],
        },
    )
    assert r.status_code == 200
    assert r.json()["valid"] is True

    # 篡改 batch_hash 必须校验失败
    forged = "f" * 64
    r = client.post(
        "/verify",
        json={
            "batch_hash": forged,
            "merkle_root": root,
            "proof_path": proof["proof_path"],
        },
    )
    assert r.json()["valid"] is False

    # 查库模式校验
    r = client.post("/verify", json={"source_id": source, "batch_no": 2})
    body = r.json()
    assert body["valid"] is True
    assert body["seal_id"] == seal_id

    # 继续封存 4（水位推进）
    r = client.post("/seals", json={"source_id": source})
    assert r.status_code == 200
    seal2 = r.json()
    assert seal2["sealed_from"] == 4
    assert seal2["sealed_through"] == 4
    assert seal2["leaf_count"] == 1
    # 单叶子树：证明路径为空，根 = leaf_hash(batch_hash)
    from app.merkle import leaf_hash

    assert seal2["proofs"][0]["proof_path"] == []
    assert seal2["merkle_root"] == leaf_hash(hashes[4])

    # 已封存批次不得再提交
    r = _submit(client, source, 0, GENESIS, _event(0))
    assert r.status_code == 409
    assert r.json()["code"] == "SEALED_BATCH"


def test_idempotent_retry_out_of_order(client):
    source = "probe-B"
    r0 = _submit(client, source, 0, GENESIS)
    h0 = r0.json()["batch_hash"]

    # 立即幂等重试（封存前）
    r0_again = _submit(client, source, 0, GENESIS)
    assert r0_again.status_code == 200
    assert r0_again.json()["duplicate"] is True
    assert r0_again.json()["batch_hash"] == h0
    assert r0_again.json()["is_sealed"] is False

    # 乱序：先到 2（前一批 1 缺失 -> 暂存，正常入库）
    r2 = _submit(client, source, 2, "a" * 64, {"x": 2})
    assert r2.status_code == 201

    gaps = client.get(f"/sources/{source}/gaps").json()
    assert gaps["missing"] == [1]
    assert gaps["next_expected_batch"] == 0
    assert gaps["contiguous_head"] == 0
    assert gaps["ready_to_seal_through"] == 0
    assert gaps["has_gap"] is True

    # 有缺口不能封存到 2
    r = client.post("/seals", json={"source_id": source, "sealed_through": 2})
    assert r.status_code == 409
    assert r.json()["code"] == "GAP_NOT_CLOSED"

    # 但可以封存到连续头 0
    r = client.post("/seals", json={"source_id": source})
    assert r.status_code == 200
    assert r.json()["sealed_through"] == 0

    # 已封存后再重试提交批次 0 -> 409（封存区间不可更改）
    r0_retry = _submit(client, source, 0, GENESIS)
    assert r0_retry.status_code == 409
    assert r0_retry.json()["code"] == "SEALED_BATCH"

    # 缺口在批次 1（水位已推进到 0，下一个期望 1）
    gaps = client.get(f"/sources/{source}/gaps").json()
    assert gaps["missing"] == [1]
    assert gaps["sealed_through"] == 0
    assert gaps["next_expected_batch"] == 1


def test_conflict_on_same_batch_no(client):
    source = "probe-C"
    _submit(client, source, 1, "a" * 64, {"v": 1})
    # 同序号但不同 prev_hash / 载荷 -> 409
    r = _submit(client, source, 1, "b" * 64, {"v": 2})
    assert r.status_code == 409
    body = r.json()
    assert body["code"] == "BATCH_CONFLICT"
    assert body["existing"]["prev_hash"] == "a" * 64
    assert body["submitted"]["prev_hash"] == "b" * 64


def test_chain_broken_reported(client):
    source = "probe-D"
    _submit(client, source, 0, GENESIS, {"v": 0})
    # 批次 1 的 prev_hash 与批次 0 不符 -> 422 但已暂存
    r = _submit(client, source, 1, "dead" + "0" * 60, {"v": 1})
    assert r.status_code == 422
    assert r.json()["code"] == "CHAIN_BROKEN"

    gaps = client.get(f"/sources/{source}/gaps").json()
    assert 1 in gaps["chain_broken"]
    # 连续头停在 0
    assert gaps["contiguous_head"] == 0

    # 封存到断裂点之后 -> 409
    r = client.post("/seals", json={"source_id": source, "sealed_through": 1})
    assert r.status_code == 409
    assert r.json()["code"] == "GAP_NOT_CLOSED"

    # 默认封存：只能封到连续头 0
    r = client.post("/seals", json={"source_id": source})
    assert r.status_code == 200
    assert r.json()["sealed_through"] == 0

    # 再次默认封存：水位之后连续链断裂（批次 1 与创世纪后无新链可封）
    r = client.post("/seals", json={"source_id": source})
    assert r.status_code == 409
    assert r.json()["code"] == "CHAIN_NOT_CLOSED"


def test_invalid_genesis(client):
    r = _submit(client, "probe-E", 0, "1" * 64)
    assert r.status_code == 422
    assert r.json()["code"] == "INVALID_GENESIS_LINK"


def test_successor_link_mismatch(client):
    source = "probe-F"
    h0 = _submit(client, source, 0, GENESIS, {"v": 0}).json()["batch_hash"]
    # 先到批次 2（乱序暂存），它声称的 prev_hash 是任意值
    r2 = _submit(client, source, 2, "c" * 64, {"v": 2})
    assert r2.status_code == 201
    # 后到批次 1，与批次 0 链接正确，但其 batch_hash 与批次 2 声称的 prev 不符
    r1 = _submit(client, source, 1, h0, {"v": 1})
    assert r1.status_code == 422
    assert r1.json()["code"] == "CHAIN_BROKEN"
    assert r1.json()["successor_batch_no"] == 2

    gaps = client.get(f"/sources/{source}/gaps").json()
    assert gaps["chain_broken"] == [2]
    assert gaps["contiguous_head"] == 1


def test_seal_empty_source(client):
    r = client.post("/seals", json={"source_id": "ghost"})
    assert r.status_code == 409
    assert r.json()["code"] == "NOTHING_TO_SEAL"


def test_merkle_root_is_deterministic_and_order_sensitive(client):
    from app.merkle import evaluate_proof, leaf_hash, merkle_proof, merkle_root

    a, b, c = "a" * 64, "b" * 64, "c" * 64
    assert merkle_root([a, b, c]) == merkle_root([a, b, c])
    assert merkle_root([a, b, c]) != merkle_root([a, c, b])
    # 单叶子：根 = leaf_hash（域分隔后的摘要），不等于裸 batch_hash
    assert merkle_root([a]) == leaf_hash(a)
    # 每个叶子证明均可独立验证
    for i, h in enumerate([a, b, c]):
        p = merkle_proof([a, b, c], i)
        assert evaluate_proof(h, p["path"]) == p["root"] == merkle_root([a, b, c])


def test_proof_not_found(client):
    r = client.get("/sources/ghost/batches/0/proof")
    assert r.status_code == 404
    assert r.json()["code"] == "SEAL_NOT_FOUND"


def test_payload_equivalence_after_reserialization(client):
    """键序不同、空白不同的等价 JSON 必须视为同一载荷。"""
    source = "probe-G"
    r1 = client.post(
        "/batches",
        json={
            "source_id": source,
            "batch_no": 0,
            "prev_hash": GENESIS,
            "payload": {"a": 1, "b": [1, 2, {"c": 3}]},
        },
    )
    h1 = r1.json()["payload_hash"]

    # 重新提交，Python dict 序列化后键序由 canonical_json 统一 -> 幂等
    r2 = client.post(
        "/batches",
        json={
            "source_id": source,
            "batch_no": 0,
            "prev_hash": GENESIS,
            "payload": {"b": [1, 2, {"c": 3}], "a": 1},
        },
    )
    assert r2.status_code == 200
    assert r2.json()["duplicate"] is True
    assert r2.json()["payload_hash"] == h1


def test_validation_errors(client):
    # 负批次号
    r = client.post(
        "/batches",
        json={"source_id": "x", "batch_no": -1, "prev_hash": GENESIS, "payload": {}},
    )
    assert r.status_code == 422

    # 非法 prev_hash
    r = client.post(
        "/batches",
        json={"source_id": "x", "batch_no": 0, "prev_hash": "zzz", "payload": {}},
    )
    assert r.status_code == 422


def test_concurrent_out_of_order_sources_and_duplicate_retries(client):
    """8 个采集端、每端 20 批，乱序并发提交；完成后每端都应能整链封存。"""
    n_sources = 8
    n_batches = 20

    # 预计算每端的合法链（批量构造时统一取 hash）
    from app.hashing import compute_batch_hash, compute_payload_hash

    chains = {}
    for s in range(n_sources):
        source = f"probe-conc-{s}"
        prev = GENESIS
        entries = []
        for n in range(n_batches):
            payload = {"source": source, "n": n}
            ph = compute_payload_hash(payload)
            bh = compute_batch_hash(source, n, prev, ph)
            entries.append((n, prev, payload, bh))
            prev = bh
        chains[source] = entries

    # 乱序：每端的批次顺序随机打乱，且每个批次额外重试一次
    jobs = []
    for source, entries in chains.items():
        ordered = list(reversed(entries))  # 全逆序提交
        for n, prev, payload, _bh in ordered:
            jobs.append((source, n, prev, payload))
            jobs.append((source, n, prev, payload))  # 并发重复提交

    def do(job):
        source, n, prev, payload = job
        return _submit(client, source, n, prev, payload)

    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        list(pool.map(do, jobs))

    # 逆序提交时，非 0 批次的前驱往往尚未到达 -> 201 暂存；
    # 前驱已到且链接不符的会 422，但随后正确批次到位……
    # 关键断言放在最终一致性：直接按正序用完整链幂等补齐，再封存。
    for source, entries in chains.items():
        for n, prev, payload, bh in entries:
            r = _submit(client, source, n, prev, payload)
            assert r.status_code in (200, 201), (source, n, r.status_code, r.text)
            assert r.json()["batch_hash"] == bh

    # 并发封存：每个源封一次整链 + 重复请求，结果必须一致
    def seal(source):
        return client.post(
            "/seals",
            json={"source_id": source, "sealed_through": n_batches - 1},
        )

    sources = list(chains)
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        seals1 = list(pool.map(seal, sources))
        seals2 = list(pool.map(seal, sources))

    roots = {}
    for source, r in zip(sources, seals1):
        assert r.status_code == 200, (source, r.text)
        body = r.json()
        assert body["sealed_through"] == n_batches - 1
        roots[source] = (body["seal_id"], body["merkle_root"])

    # 重复封存全部幂等命中，根与 seal_id 完全一致
    for source, r in zip(sources, seals2):
        body = r.json()
        assert body["already_sealed"] is True
        assert (body["seal_id"], body["merkle_root"]) == roots[source]
