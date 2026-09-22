"""请求/响应模型。"""

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator

from .hashing import GENESIS_HASH

HEX64_PATTERN = r"^[0-9a-fA-F]{64}$"


class BatchSubmission(BaseModel):
    source_id: str = Field(..., min_length=1, description="采集端来源标识")
    batch_no: int = Field(..., ge=0, description="批次序号，从 0 开始，按来源严格递增")
    prev_hash: str = Field(
        ...,
        description=f"前一批次 batch_hash；第 0 批固定为 {GENESIS_HASH}",
    )
    payload: Any = Field(..., description="任意 JSON 事件载荷")

    @field_validator("source_id")
    @classmethod
    def _source_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("source_id 不能为空串")
        return v

    @field_validator("prev_hash")
    @classmethod
    def _prev_hash_hex64(cls, v: str) -> str:
        if len(v) != 64:
            raise ValueError("prev_hash 必须是 64 位十六进制 SHA-256 摘要")
        int(v, 16)
        return v.lower()


class ProofStep(BaseModel):
    sibling: str = Field(..., description="兄弟节点摘要")
    position: Literal["left", "right"] = Field(
        ..., description="兄弟节点相对当前节点的位置"
    )


class BatchProof(BaseModel):
    source_id: str
    batch_no: int
    batch_hash: str
    index: int = Field(..., description="该批次在封存叶子序列中的下标")
    sealed_from: int
    sealed_through: int
    merkle_root: str
    seal_id: str
    proof_path: list[ProofStep] = Field(default_factory=list)


class SubmitResponse(BaseModel):
    source_id: str
    batch_no: int
    batch_hash: str
    payload_hash: str
    duplicate: bool = Field(..., description="true 表示该批次已存在（幂等重试）")
    is_sealed: bool = Field(..., description="该批次是否已经封存")


class GapResponse(BaseModel):
    source_id: str
    sealed_through: int = Field(..., description="已封存水位（无封存时为 -1）")
    next_expected_batch: int
    highest_received: int = Field(
        ..., description="已收到的最大批次号（无数据时为 -1）"
    )
    missing: list[int] = Field(..., description="缺失批次号（受 max_batch 限制）")
    chain_broken: list[int] = Field(
        ..., description="prev_hash 与前一已存批次不符的批次号"
    )
    contiguous_head: int = Field(..., description="自水位起连续链可达的最大批次号")
    ready_to_seal_through: int = Field(..., description="当前可立即封存至的批次号")
    has_gap: bool


class SealRequest(BaseModel):
    source_id: str = Field(..., min_length=1)
    sealed_through: Optional[int] = Field(
        None,
        ge=0,
        description="拟封存的末尾批次号；缺省表示封存到当前连续链头",
    )

    @field_validator("source_id")
    @classmethod
    def _source_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("source_id 不能为空串")
        return v


class SealResponse(BaseModel):
    seal_id: str
    source_id: str
    sealed_from: int
    sealed_through: int
    merkle_root: str
    leaf_count: int
    created_at: str
    already_sealed: bool = Field(..., description="true 表示命中既有封存（幂等）")
    proofs: list[BatchProof] = Field(..., description="本封存区间内每个批次的包含证明")


class VerifyRequest(BaseModel):
    """校验请求：既可让服务端查库校验，也可只凭外部传入的证明复核。"""

    batch_hash: Optional[str] = Field(
        None,
        description="待校验批次的 batch_hash；查库模式可省略，由服务端读取",
    )
    merkle_root: Optional[str] = Field(None, description="待匹配的 Merkle 根")
    proof_path: Optional[list[ProofStep]] = Field(
        None,
        description="包含证明路径；缺省时由服务端按 source_id/batch_no 查库生成",
    )
    source_id: Optional[str] = Field(None, description="查库模式的来源标识")
    batch_no: Optional[int] = Field(None, ge=0, description="查库模式的批次号")
    sealed_through: Optional[int] = Field(
        None, ge=0, description="限定查库模式下的封存区间"
    )

    @field_validator("batch_hash")
    @classmethod
    def _batch_hash_hex64(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            if len(v) != 64:
                raise ValueError("batch_hash 必须是 64 位十六进制 SHA-256 摘要")
            int(v, 16)
            return v.lower()
        return v

    @field_validator("merkle_root")
    @classmethod
    def _root_hex64(cls, v: Optional[str]) -> Optional[str]:
        if v is not None:
            if len(v) != 64:
                raise ValueError("merkle_root 必须是 64 位十六进制摘要")
            int(v, 16)
            return v.lower()
        return v


class VerifyResponse(BaseModel):
    valid: bool
    batch_hash: str
    computed_root: str
    expected_root: Optional[str] = None
    seal_id: Optional[str] = None
    stored_batch_hash_matches: Optional[bool] = Field(
        None, description="查库模式：批次是否存在且 batch_hash 一致"
    )
    reason: Optional[str] = Field(None, description="valid=false 时的原因说明")
