"""确定性哈希工具。

所有哈希均为 SHA-256 的十六进制摘要（64 个小写字符）。
规范化 JSON 用于保证不同采集端、不同语言对同一载荷得到完全一致的摘要。
"""

import hashlib
import json
from typing import Any

# 第 0 号批次必须使用的前序哈希（创世纪链接）。
GENESIS_HASH = "0" * 64


def canonical_json_bytes(value: Any) -> bytes:
    """将任意 JSON 值序列化为确定性字节串。

    - 键按字典序排序
    - 无多余空白
    - 不转义非 ASCII 字符
    - 拒绝 NaN / Infinity（非合法 JSON 令牌）
    """
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def compute_payload_hash(payload: Any) -> str:
    """payload_hash = SHA256(canonical_json(payload))"""
    return sha256_hex(canonical_json_bytes(payload))


def compute_batch_hash(
    source_id: str,
    batch_no: int,
    prev_hash: str,
    payload_hash: str,
) -> str:
    """batch_hash 绑定来源、序号、前序哈希与载荷摘要。

    预image采用带版本域分隔的规范化 JSON 数组，避免字段拼接歧义：
        SHA256(["BATCH_V1", source_id, batch_no, prev_hash, payload_hash])
    """
    preimage = canonical_json_bytes(
        ["BATCH_V1", source_id, batch_no, prev_hash, payload_hash]
    )
    return sha256_hex(preimage)
