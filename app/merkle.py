"""确定性 Merkle 树。

约定（封存与校验双方都必须遵守）：
1. 叶子顺序严格按 batch_no 升序；
2. 叶子摘要 = SHA256("MERKLE_LEAF_V1" || batch_hash_bytes)，做域分隔防止
   内部节点/叶子伪造攻击；
3. 内部节点 = SHA256("MERKLE_INTERNAL_V1" || left_bytes || right_bytes)；
4. 奇数节点时最后一个节点与自身配对（Bitcoin 风格），因此单叶子树的根
   就是该叶子摘要，包含证明路径为空。

输入/输出哈希一律为 64 位小写十六进制字符串。
"""

from hashlib import sha256
from typing import Any

LEAF_DOMAIN = b"MERKLE_LEAF_V1"
NODE_DOMAIN = b"MERKLE_INTERNAL_V1"


def _to_bytes(hex_hash: str) -> bytes:
    return bytes.fromhex(hex_hash)


def leaf_hash(batch_hash_hex: str) -> str:
    return sha256(LEAF_DOMAIN + _to_bytes(batch_hash_hex)).hexdigest()


def _internal_hash(left_hex: str, right_hex: str) -> str:
    return sha256(NODE_DOMAIN + _to_bytes(left_hex) + _to_bytes(right_hex)).hexdigest()


def _leaf_level(batch_hashes: list[str]) -> list[str]:
    if not batch_hashes:
        raise ValueError("Merkle 树至少需要一个叶子")
    return [leaf_hash(h) for h in batch_hashes]


def _next_level(level: list[str]) -> list[str]:
    nxt: list[str] = []
    for i in range(0, len(level), 2):
        left = level[i]
        right = level[i + 1] if i + 1 < len(level) else level[i]
        nxt.append(_internal_hash(left, right))
    return nxt


def merkle_root(batch_hashes: list[str]) -> str:
    """由 batch_hash 列表计算确定性 Merkle 根。"""
    level = _leaf_level(batch_hashes)
    while len(level) > 1:
        level = _next_level(level)
    return level[0]


def merkle_proof(batch_hashes: list[str], index: int) -> dict[str, Any]:
    """生成指定下标的 Merkle 包含证明。

    返回 {"root": ..., "path": [{"sibling": ..., "position": "left|right"}]}。
    position 表示兄弟节点相对于当前节点的位置，校验时据此决定拼接顺序。
    """
    if not 0 <= index < len(batch_hashes):
        raise IndexError("叶子下标越界")

    level = _leaf_level(batch_hashes)
    idx = index
    path: list[dict[str, str]] = []

    while len(level) > 1:
        if idx % 2 == 0:
            # 左节点：兄弟在右；若为孤立尾节点则复制自身。
            sibling = level[idx + 1] if idx + 1 < len(level) else level[idx]
            position = "right"
        else:
            sibling = level[idx - 1]
            position = "left"
        path.append({"sibling": sibling, "position": position})

        level = _next_level(level)
        idx //= 2

    return {"root": level[0], "path": path}


def evaluate_proof(batch_hash_hex: str, path: list[dict[str, str]]) -> str:
    """仅凭 batch_hash 与证明路径重新计算根（不依赖本服务/数据库）。"""
    acc = leaf_hash(batch_hash_hex)
    for step in path:
        position = step["position"]
        sibling = step["sibling"]
        if position == "left":
            acc = _internal_hash(sibling, acc)
        elif position == "right":
            acc = _internal_hash(acc, sibling)
        else:
            raise ValueError(f"非法的证明路径方向: {position!r}")
    return acc
