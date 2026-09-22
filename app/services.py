"""业务逻辑：乱序暂存、幂等去重、连续链检测、封存与校验。"""

import sqlite3
from datetime import UTC, datetime
from typing import Any, Optional

from . import merkle
from .database import write_lock
from .hashing import (
    GENESIS_HASH,
    canonical_json_bytes,
    compute_batch_hash,
    compute_payload_hash,
    sha256_hex,
)
from .models import BatchProof, ProofStep


class ServiceError(Exception):
    """业务错误，携带 HTTP 状态码与结构化详情。"""

    def __init__(self, status_code: int, code: str, message: str, **extra: Any):
        self.status_code = status_code
        self.code = code
        self.detail: dict[str, Any] = {"code": code, "message": message, **extra}
        super().__init__(message)


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _sealed_through(conn: sqlite3.Connection, source_id: str) -> int:
    """来源当前封存水位。"""
    row = conn.execute(
        "SELECT COALESCE(MAX(sealed_through), -1) AS m FROM seals WHERE source_id = ?",
        (source_id,),
    ).fetchone()
    return int(row["m"])


def _get_batch(conn: sqlite3.Connection, source_id: str, batch_no: int):
    return conn.execute(
        "SELECT * FROM batches WHERE source_id = ? AND batch_no = ?",
        (source_id, batch_no),
    ).fetchone()


def _walk_contiguous(conn: sqlite3.Connection, source_id: str, start: int) -> int:
    """从 start 开始，沿 prev_hash 链接行走，返回连续链可达的最大批次号。

    start 必须存在且与前驱（创世纪或 start-1）链接正确；否则返回 start - 1。
    """
    cur = _get_batch(conn, source_id, start)
    if cur is None:
        return start - 1
    if start == 0:
        if cur["prev_hash"] != GENESIS_HASH:
            return -1
    else:
        prior = _get_batch(conn, source_id, start - 1)
        if prior is None or cur["prev_hash"] != prior["batch_hash"]:
            return start - 1
    n = start
    while True:
        nxt = _get_batch(conn, source_id, n + 1)
        if nxt is None or nxt["prev_hash"] != cur["batch_hash"]:
            return n
        cur = nxt
        n += 1


def submit_batch(
    conn: sqlite3.Connection,
    *,
    source_id: str,
    batch_no: int,
    prev_hash: str,
    payload: Any,
) -> dict[str, Any]:
    """接收一个批次（允许乱序、重试），返回提交结果。

    冲突规则：
    - 相同 (source_id, batch_no) 且内容完全一致 -> 幂等成功（200）；
    - 相同键但 prev_hash / payload 不一致 -> 409 冲突；
    - 新批次的 prev_hash 与已存在的前一批次 batch_hash 不符 -> 422 断链
      （仍会入库暂存，等待调查，因为后到的"正确"批次才是冲突对象）；
    - 已存在的后一批次 prev_hash 指向与新批次 batch_hash 不符 -> 422；
    - 第 0 批 prev_hash 必须为 GENESIS，否则 422；
    - 已封存区间内不允许提交（含冲突重提），返回 409。
    """
    payload_hash = compute_payload_hash(payload)
    batch_hash = compute_batch_hash(source_id, batch_no, prev_hash, payload_hash)
    payload_json = canonical_json_bytes(payload).decode("utf-8")

    with write_lock:
        conn.execute("BEGIN IMMEDIATE")
        try:
            sealed_through = _sealed_through(conn, source_id)
            if batch_no <= sealed_through:
                raise ServiceError(
                    409,
                    "SEALED_BATCH",
                    f"批次 {batch_no} 已封存，封存区间不可更改",
                    source_id=source_id,
                    batch_no=batch_no,
                    sealed_through=sealed_through,
                )

            existing = _get_batch(conn, source_id, batch_no)
            if existing is not None:
                if (
                    existing["prev_hash"] == prev_hash
                    and existing["payload_hash"] == payload_hash
                    and existing["batch_hash"] == batch_hash
                ):
                    return {
                        "source_id": source_id,
                        "batch_no": batch_no,
                        "batch_hash": batch_hash,
                        "payload_hash": payload_hash,
                        "duplicate": True,
                        "is_sealed": bool(existing["is_sealed"]),
                        "status_code": 200,
                    }
                raise ServiceError(
                    409,
                    "BATCH_CONFLICT",
                    f"批次 {batch_no} 已存在但内容不一致",
                    source_id=source_id,
                    batch_no=batch_no,
                    existing={
                        "prev_hash": existing["prev_hash"],
                        "payload_hash": existing["payload_hash"],
                        "batch_hash": existing["batch_hash"],
                    },
                    submitted={
                        "prev_hash": prev_hash,
                        "payload_hash": payload_hash,
                        "batch_hash": batch_hash,
                    },
                )

            if batch_no == 0 and prev_hash != GENESIS_HASH:
                raise ServiceError(
                    422,
                    "INVALID_GENESIS_LINK",
                    f"第 0 批的 prev_hash 必须为 {GENESIS_HASH}",
                    source_id=source_id,
                    batch_no=batch_no,
                )

            prior = _get_batch(conn, source_id, batch_no - 1) if batch_no > 0 else None
            if prior is not None and prev_hash != prior["batch_hash"]:
                # 先写入暂存，再明确报错，避免重试造成缺口误判。
                conn.execute(
                    """INSERT INTO batches
                       (source_id, batch_no, prev_hash, payload_hash, batch_hash,
                        payload_json, received_at, is_sealed)
                       VALUES (?, ?, ?, ?, ?, ?, ?, 0)""",
                    (
                        source_id,
                        batch_no,
                        prev_hash,
                        payload_hash,
                        batch_hash,
                        payload_json,
                        _now(),
                    ),
                )
                conn.commit()
                raise ServiceError(
                    422,
                    "CHAIN_BROKEN",
                    f"批次 {batch_no} 的 prev_hash 与批次 {batch_no - 1} 不符",
                    source_id=source_id,
                    batch_no=batch_no,
                    expected_prev_hash=prior["batch_hash"],
                    submitted_prev_hash=prev_hash,
                )

            successor = _get_batch(conn, source_id, batch_no + 1)
            if successor is not None and successor["prev_hash"] != batch_hash:
                conn.execute(
                    """INSERT INTO batches
                       (source_id, batch_no, prev_hash, payload_hash, batch_hash,
                        payload_json, received_at, is_sealed)
                       VALUES (?, ?, ?, ?, ?, ?, ?, 0)""",
                    (
                        source_id,
                        batch_no,
                        prev_hash,
                        payload_hash,
                        batch_hash,
                        payload_json,
                        _now(),
                    ),
                )
                conn.commit()
                raise ServiceError(
                    422,
                    "CHAIN_BROKEN",
                    f"后一批次 {batch_no + 1} 记录的前序链接与本批次不符",
                    source_id=source_id,
                    batch_no=batch_no,
                    successor_batch_no=batch_no + 1,
                    successor_prev_hash=successor["prev_hash"],
                    computed_batch_hash=batch_hash,
                )

            conn.execute(
                """INSERT INTO batches
                   (source_id, batch_no, prev_hash, payload_hash, batch_hash,
                    payload_json, received_at, is_sealed)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 0)""",
                (
                    source_id,
                    batch_no,
                    prev_hash,
                    payload_hash,
                    batch_hash,
                    payload_json,
                    _now(),
                ),
            )
            conn.commit()
            return {
                "source_id": source_id,
                "batch_no": batch_no,
                "batch_hash": batch_hash,
                "payload_hash": payload_hash,
                "duplicate": False,
                "is_sealed": False,
                "status_code": 201,
            }
        except sqlite3.IntegrityError:
            # 并发重复提交：两个相同 INSERT 竞争，败者在此重查，
            # 按幂等成功（内容一致）或冲突（内容不一致）处理。
            conn.rollback()
            existing = _get_batch(conn, source_id, batch_no)
            if existing is None:
                raise
            if (
                existing["prev_hash"] == prev_hash
                and existing["payload_hash"] == payload_hash
                and existing["batch_hash"] == batch_hash
            ):
                return {
                    "source_id": source_id,
                    "batch_no": batch_no,
                    "batch_hash": batch_hash,
                    "payload_hash": payload_hash,
                    "duplicate": True,
                    "is_sealed": bool(existing["is_sealed"]),
                    "status_code": 200,
                }
            raise ServiceError(
                409,
                "BATCH_CONFLICT",
                f"批次 {batch_no} 已存在但内容不一致",
                source_id=source_id,
                batch_no=batch_no,
                existing={
                    "prev_hash": existing["prev_hash"],
                    "payload_hash": existing["payload_hash"],
                    "batch_hash": existing["batch_hash"],
                },
                submitted={
                    "prev_hash": prev_hash,
                    "payload_hash": payload_hash,
                    "batch_hash": batch_hash,
                },
            )
        except ServiceError:
            raise
        except Exception:
            conn.rollback()
            raise


def get_gaps(
    conn: sqlite3.Connection,
    source_id: str,
    max_batch: Optional[int] = None,
) -> dict[str, Any]:
    """查询来源的缺口、断链点与可封存水位。"""
    sealed_through = _sealed_through(conn, source_id)

    highest = conn.execute(
        "SELECT COALESCE(MAX(batch_no), -1) AS m FROM batches WHERE source_id = ?",
        (source_id,),
    ).fetchone()["m"]

    all_rows = conn.execute(
        "SELECT batch_no, prev_hash, batch_hash FROM batches "
        "WHERE source_id = ? ORDER BY batch_no",
        (source_id,),
    ).fetchall()
    present = {r["batch_no"]: r for r in all_rows}

    chain_broken: list[int] = []
    for n, r in present.items():
        if n <= sealed_through:
            continue
        if n == 0:
            if r["prev_hash"] != GENESIS_HASH:
                chain_broken.append(0)
        else:
            pred = present.get(n - 1)
            if pred is not None and r["prev_hash"] != pred["batch_hash"]:
                chain_broken.append(n)

    start = sealed_through + 1
    head = (
        _walk_contiguous(conn, source_id, start)
        if _get_batch(conn, source_id, start) is not None
        else sealed_through
    )

    upper = max_batch if max_batch is not None else highest
    missing: list[int] = []
    if upper >= start:
        missing = [n for n in range(start, upper + 1) if n not in present]

    ready = head
    return {
        "source_id": source_id,
        "sealed_through": sealed_through,
        "next_expected_batch": start,
        "highest_received": highest,
        "missing": missing,
        "chain_broken": chain_broken,
        "contiguous_head": head,
        "ready_to_seal_through": ready,
        "has_gap": bool(missing) or head < start,
    }


def _deterministic_seal_id(
    source_id: str, sealed_from: int, sealed_through: int, merkle_root_hex: str
) -> str:
    """封存标识也确定化：同样的区间与根 -> 同样的 seal_id（便于审计/重试）。"""
    material = canonical_json_bytes(
        ["SEAL_V1", source_id, sealed_from, sealed_through, merkle_root_hex]
    )
    return sha256_hex(material)[:32]


def _to_batch_proof(
    *,
    source_id: str,
    batch_no: int,
    batch_hash_hex: str,
    index: int,
    sealed_from: int,
    sealed_through: int,
    merkle_root_hex: str,
    seal_id: str,
    proof_path: list[dict[str, str]],
) -> BatchProof:
    return BatchProof(
        source_id=source_id,
        batch_no=batch_no,
        batch_hash=batch_hash_hex,
        index=index,
        sealed_from=sealed_from,
        sealed_through=sealed_through,
        merkle_root=merkle_root_hex,
        seal_id=seal_id,
        proof_path=[ProofStep(**step) for step in proof_path],
    )


def _build_proofs_for_seal(
    conn: sqlite3.Connection, seal: sqlite3.Row
) -> list[BatchProof]:
    """根据已存在的封存记录重建全部叶子的包含证明（证明确定性可复算）。"""
    rows = conn.execute(
        "SELECT batch_no, batch_hash FROM batches "
        "WHERE source_id = ? AND batch_no BETWEEN ? AND ? ORDER BY batch_no",
        (seal["source_id"], seal["sealed_from"], seal["sealed_through"]),
    ).fetchall()
    leaves = [r["batch_hash"] for r in rows]
    root = seal["merkle_root"]
    proofs: list[BatchProof] = []
    for i, r in enumerate(rows):
        mp = merkle.merkle_proof(leaves, i)
        if mp["root"] != root:
            raise ServiceError(
                500,
                "INTERNAL_ERROR",
                "复算 Merkle 根与封存记录不一致，数据可能损坏",
                source_id=seal["source_id"],
                seal_id=seal["seal_id"],
            )
        proofs.append(
            _to_batch_proof(
                source_id=seal["source_id"],
                batch_no=r["batch_no"],
                batch_hash_hex=r["batch_hash"],
                index=i,
                sealed_from=seal["sealed_from"],
                sealed_through=seal["sealed_through"],
                merkle_root_hex=root,
                seal_id=seal["seal_id"],
                proof_path=mp["path"],
            )
        )
    return proofs


def seal_batches(
    conn: sqlite3.Connection, source_id: str, sealed_through_req: Optional[int]
) -> dict[str, Any]:
    """封存连续链区间并返回每个批次的 Merkle 包含证明。"""
    with write_lock:
        conn.execute("BEGIN IMMEDIATE")
        try:
            wm = _sealed_through(conn, source_id)

            # 幂等：重复请求封存已封存区间 -> 直接返回既有封存与可复算证明。
            if sealed_through_req is not None and sealed_through_req <= wm:
                existing = conn.execute(
                    "SELECT * FROM seals WHERE source_id = ? AND sealed_through = ?",
                    (source_id, sealed_through_req),
                ).fetchone()
                if existing is None:
                    raise ServiceError(
                        422,
                        "INVALID_SEAL_RANGE",
                        f"批次 {sealed_through_req} 已越过封存水位 {wm}，"
                        "但不存在对应的封存记录",
                        source_id=source_id,
                        sealed_through=sealed_through_req,
                        watermark=wm,
                    )
                return {
                    "seal_id": existing["seal_id"],
                    "source_id": source_id,
                    "sealed_from": existing["sealed_from"],
                    "sealed_through": existing["sealed_through"],
                    "merkle_root": existing["merkle_root"],
                    "leaf_count": existing["leaf_count"],
                    "created_at": existing["created_at"],
                    "already_sealed": True,
                    "proofs": _build_proofs_for_seal(conn, existing),
                }

            start = wm + 1

            head = (
                _walk_contiguous(conn, source_id, start)
                if _get_batch(conn, source_id, start) is not None
                else wm
            )

            if head < start:
                if _get_batch(conn, source_id, start) is None:
                    raise ServiceError(
                        409,
                        "NOTHING_TO_SEAL",
                        f"批次 {start} 尚未收到，连续链未闭合，无法封存",
                        source_id=source_id,
                        next_expected_batch=start,
                        sealed_through=wm,
                    )
                raise ServiceError(
                    409,
                    "CHAIN_NOT_CLOSED",
                    f"自批次 {start} 起的哈希链不连续，无法封存",
                    source_id=source_id,
                    next_expected_batch=start,
                    contiguous_head=head,
                    sealed_through=wm,
                )

            end = head if sealed_through_req is None else sealed_through_req
            if end > head:
                gaps = get_gaps(conn, source_id, max_batch=end)
                raise ServiceError(
                    409,
                    "GAP_NOT_CLOSED",
                    f"批次 {end} 之前仍存在缺口，连续链未闭合",
                    source_id=source_id,
                    requested_through=end,
                    contiguous_head=head,
                    missing=gaps["missing"],
                    next_expected_batch=start,
                )
            if end < start:
                raise ServiceError(
                    422,
                    "INVALID_SEAL_RANGE",
                    f"sealed_through 不能小于下一批次 {start}",
                    source_id=source_id,
                    sealed_through=end,
                    next_expected_batch=start,
                )

            rows = conn.execute(
                "SELECT batch_no, batch_hash FROM batches "
                "WHERE source_id = ? AND batch_no BETWEEN ? AND ? "
                "ORDER BY batch_no",
                (source_id, start, end),
            ).fetchall()
            leaf_hashes_input = [r["batch_hash"] for r in rows]
            root = merkle.merkle_root(leaf_hashes_input)
            seal_id = _deterministic_seal_id(source_id, start, end, root)
            created_at = _now()

            conn.execute(
                """INSERT INTO seals
                   (seal_id, source_id, sealed_from, sealed_through, merkle_root,
                    leaf_count, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (
                    seal_id,
                    source_id,
                    start,
                    end,
                    root,
                    len(rows),
                    created_at,
                ),
            )
            conn.execute(
                "UPDATE batches SET is_sealed = 1 "
                "WHERE source_id = ? AND batch_no BETWEEN ? AND ?",
                (source_id, start, end),
            )
            conn.commit()

            proofs: list[BatchProof] = []
            for i, r in enumerate(rows):
                mp = merkle.merkle_proof(leaf_hashes_input, i)
                proofs.append(
                    _to_batch_proof(
                        source_id=source_id,
                        batch_no=r["batch_no"],
                        batch_hash_hex=r["batch_hash"],
                        index=i,
                        sealed_from=start,
                        sealed_through=end,
                        merkle_root_hex=root,
                        seal_id=seal_id,
                        proof_path=mp["path"],
                    )
                )

            return {
                "seal_id": seal_id,
                "source_id": source_id,
                "sealed_from": start,
                "sealed_through": end,
                "merkle_root": root,
                "leaf_count": len(rows),
                "created_at": created_at,
                "already_sealed": False,
                "proofs": proofs,
            }
        except ServiceError:
            raise
        except Exception:
            conn.rollback()
            raise


def _fetch_seal(
    conn: sqlite3.Connection,
    source_id: str,
    sealed_through: Optional[int],
    batch_no: Optional[int] = None,
):
    if sealed_through is None and batch_no is not None:
        # 未指定封存区间时，取覆盖该批次的那一段封存。
        return conn.execute(
            "SELECT * FROM seals WHERE source_id = ? "
            "AND sealed_from <= ? AND ? <= sealed_through "
            "ORDER BY sealed_through DESC LIMIT 1",
            (source_id, batch_no, batch_no),
        ).fetchone()
    if sealed_through is None:
        return conn.execute(
            "SELECT * FROM seals WHERE source_id = ? "
            "ORDER BY sealed_through DESC LIMIT 1",
            (source_id,),
        ).fetchone()
    return conn.execute(
        "SELECT * FROM seals WHERE source_id = ? AND sealed_through = ?",
        (source_id, sealed_through),
    ).fetchone()


def get_proof(
    conn: sqlite3.Connection,
    source_id: str,
    batch_no: int,
    sealed_through: Optional[int],
) -> BatchProof:
    """查询某已封存批次的包含证明。"""
    seal = _fetch_seal(conn, source_id, sealed_through, batch_no)
    if seal is None:
        raise ServiceError(
            404,
            "SEAL_NOT_FOUND",
            "未找到符合条件的封存记录",
            source_id=source_id,
            sealed_through=sealed_through,
        )
    if not (seal["sealed_from"] <= batch_no <= seal["sealed_through"]):
        raise ServiceError(
            404,
            "BATCH_NOT_IN_SEAL",
            f"批次 {batch_no} 不在封存区间 "
            f"[{seal['sealed_from']}, {seal['sealed_through']}] 内",
            source_id=source_id,
            batch_no=batch_no,
            sealed_from=seal["sealed_from"],
            sealed_through=seal["sealed_through"],
        )

    rows = conn.execute(
        "SELECT batch_no, batch_hash FROM batches "
        "WHERE source_id = ? AND batch_no BETWEEN ? AND ? ORDER BY batch_no",
        (source_id, seal["sealed_from"], seal["sealed_through"]),
    ).fetchall()
    leaves = [r["batch_hash"] for r in rows]
    index = batch_no - seal["sealed_from"]
    if leaves[index] != rows[index]["batch_hash"]:  # 防御性检查，实际恒等
        raise ServiceError(500, "INTERNAL_ERROR", "封存叶子与批次记录不一致")

    mp = merkle.merkle_proof(leaves, index)
    return _to_batch_proof(
        source_id=source_id,
        batch_no=batch_no,
        batch_hash_hex=leaves[index],
        index=index,
        sealed_from=seal["sealed_from"],
        sealed_through=seal["sealed_through"],
        merkle_root_hex=seal["merkle_root"],
        seal_id=seal["seal_id"],
        proof_path=mp["path"],
    )


def verify_proof(conn: sqlite3.Connection, data: Any) -> dict[str, Any]:
    """校验包含证明。

    - 提供 proof_path 时完全离线式复核（只用传入数据）；
    - 否则按 source_id + batch_no 查库生成证明再复核。
    """
    # 查库模式
    if data.proof_path is None:
        if data.source_id is None or data.batch_no is None:
            raise ServiceError(
                422,
                "MISSING_LOOKUP_KEYS",
                "未提供 proof_path 时必须同时提供 source_id 与 batch_no",
            )

        stored = _get_batch(conn, data.source_id, data.batch_no)
        if stored is None:
            return {
                "valid": False,
                "batch_hash": data.batch_hash,
                "computed_root": None,
                "expected_root": data.merkle_root,
                "seal_id": None,
                "stored_batch_hash_matches": False,
                "reason": "库中不存在该批次",
            }

        stored_hash = stored["batch_hash"]
        submitted_hash = data.batch_hash or stored_hash
        stored_matches = submitted_hash == stored_hash

        try:
            proof = get_proof(conn, data.source_id, data.batch_no, data.sealed_through)
        except ServiceError as exc:
            return {
                "valid": False,
                "batch_hash": submitted_hash,
                "computed_root": None,
                "expected_root": data.merkle_root,
                "seal_id": None,
                "stored_batch_hash_matches": stored_matches,
                "reason": exc.detail["message"],
            }

        path = [step.model_dump() for step in proof.proof_path]
        computed = merkle.evaluate_proof(submitted_hash, path)
        expected = data.merkle_root or proof.merkle_root
        valid = stored_matches and computed == expected == proof.merkle_root
        reason = None
        if not stored_matches:
            reason = "提交的 batch_hash 与库中已封存记录不一致"
        elif computed != expected:
            reason = "重算根与封存根不一致"
        return {
            "valid": valid,
            "batch_hash": submitted_hash,
            "computed_root": computed,
            "expected_root": expected,
            "seal_id": proof.seal_id,
            "stored_batch_hash_matches": stored_matches,
            "reason": reason,
        }

    # 外部证明模式
    if data.batch_hash is None:
        raise ServiceError(
            422,
            "MISSING_BATCH_HASH",
            "提供 proof_path 时必须同时提供 batch_hash",
        )
    path = [step.model_dump() for step in data.proof_path]
    computed = merkle.evaluate_proof(data.batch_hash, path)
    if data.merkle_root is None:
        return {
            "valid": False,
            "batch_hash": data.batch_hash,
            "computed_root": computed,
            "expected_root": None,
            "seal_id": None,
            "stored_batch_hash_matches": None,
            "reason": "外部证明校验必须同时提供 merkle_root",
        }
    return {
        "valid": computed == data.merkle_root,
        "batch_hash": data.batch_hash,
        "computed_root": computed,
        "expected_root": data.merkle_root,
        "seal_id": None,
        "stored_batch_hash_matches": None,
        "reason": None if computed == data.merkle_root else "重算根与给定根不一致",
    }
