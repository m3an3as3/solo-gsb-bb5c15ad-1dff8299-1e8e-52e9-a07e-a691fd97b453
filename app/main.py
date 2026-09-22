"""FastAPI 入口：提交 / 缺口查询 / 封存 / 证明查询 / 校验。"""

from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import Depends, FastAPI, Query, Response
from fastapi.responses import JSONResponse

from . import services
from .config import settings
from .database import get_conn, init_db
from .models import (
    BatchProof,
    BatchSubmission,
    GapResponse,
    SealRequest,
    SealResponse,
    SubmitResponse,
    VerifyRequest,
    VerifyResponse,
)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    conn = get_conn()
    try:
        init_db(conn)
    finally:
        conn.close()
    yield


app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    description=(
        "多采集端证据批次封存服务：乱序暂存、幂等去重、哈希链连续闭合后封存，"
        "封存产出确定性 Merkle 根与包含证明。"
    ),
    lifespan=lifespan,
)


def db() -> Any:
    conn = get_conn()
    try:
        yield conn
    finally:
        conn.close()


@app.exception_handler(services.ServiceError)
async def service_error_handler(
    _request: Any, exc: services.ServiceError
) -> JSONResponse:
    return JSONResponse(status_code=exc.status_code, content=exc.detail)


@app.get("/health", tags=["运维"], summary="健康检查")
def health(conn: Any = Depends(db)) -> dict[str, Any]:
    row = conn.execute("SELECT 1 AS ok").fetchone()
    return {"status": "ok", "db": "sqlite", "check": row["ok"]}


@app.post(
    "/batches",
    response_model=SubmitResponse,
    tags=["提交"],
    summary="提交（上传）一个事件批次",
    responses={
        200: {"description": "幂等重试：批次已存在且内容完全一致"},
        201: {"description": "新批次已暂存"},
        409: {"description": "内容冲突 / 已封存 / 链未闭合相关冲突"},
        422: {"description": "哈希链断裂或创世纪链接非法"},
    },
)
def submit_batch(
    payload: BatchSubmission, response: Response, conn: Any = Depends(db)
) -> SubmitResponse:
    result = services.submit_batch(
        conn,
        source_id=payload.source_id,
        batch_no=payload.batch_no,
        prev_hash=payload.prev_hash,
        payload=payload.payload,
    )
    response.status_code = result.pop("status_code")
    return SubmitResponse(**result)


@app.get(
    "/sources/{source_id}/gaps",
    response_model=GapResponse,
    tags=["缺口"],
    summary="查询某来源的缺口、断链点与可封存水位",
)
def get_gaps(
    source_id: str,
    max_batch: Optional[int] = Query(
        None,
        ge=0,
        description="检查上限；缺省只统计到已收到的最大批次",
    ),
    conn: Any = Depends(db),
) -> GapResponse:
    return GapResponse(**services.get_gaps(conn, source_id, max_batch))


@app.post(
    "/seals",
    response_model=SealResponse,
    tags=["封存"],
    summary="封存当前连续链（或封存到指定批次）",
    responses={
        409: {"description": "链未闭合 / 仍有缺口 / 暂无可封存批次"},
        422: {"description": "封存范围非法"},
    },
)
def seal_batches(payload: SealRequest, conn: Any = Depends(db)) -> SealResponse:
    result = services.seal_batches(conn, payload.source_id, payload.sealed_through)
    return SealResponse(**result)


@app.get(
    "/sources/{source_id}/batches/{batch_no}/proof",
    response_model=BatchProof,
    tags=["校验"],
    summary="查询某已封存批次的 Merkle 包含证明",
    responses={404: {"description": "封存或批次不存在"}},
)
def get_batch_proof(
    source_id: str,
    batch_no: int,
    sealed_through: Optional[int] = Query(
        None, description="封存区间末尾批次；缺省取该来源最新封存"
    ),
    conn: Any = Depends(db),
) -> BatchProof:
    return services.get_proof(conn, source_id, batch_no, sealed_through)


@app.post(
    "/verify",
    response_model=VerifyResponse,
    tags=["校验"],
    summary="校验 Merkle 包含证明（支持离线证明或服务端查库）",
)
def verify(payload: VerifyRequest, conn: Any = Depends(db)) -> VerifyResponse:
    return VerifyResponse(**services.verify_proof(conn, payload))
