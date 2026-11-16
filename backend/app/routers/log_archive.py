"""调用日志归档接口：归档窗口、数据集复核、审计/契约消费与文件导出下载。

处理链路：POST /api/log-archive/windows 触发
标准化日志 → 归档数据集 → 检查点校验 → 审计记录 + 日志归档契约 同事务提交。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Response
from pydantic import BaseModel

from app.schemas import ActionResult, PageResult
from app.services.log_archive import (
    ArchiveConflictError,
    ArchiveVerifyError,
    log_archive_service,
)

router = APIRouter(prefix="/api/log-archive", tags=["调用日志归档"])


class ArchiveRequest(BaseModel):
    window_id: str | None = None
    source_number: str | None = None
    simulate_failure: str | None = None  # stage / commit / verify，用于回滚重做演练


def _fail(exc: Exception, status: int = 400) -> HTTPException:
    return HTTPException(status_code=status, detail=str(exc))


@router.get("/stats", response_model=dict)
def archive_stats() -> dict[str, Any]:
    """归档链路总览：窗口、数据集、审计、契约与回滚次数。"""
    return log_archive_service.stats()


@router.get("/windows", response_model=PageResult[dict])
def list_windows(
    keyword: str | None = Query(default=None, description="按归档窗口编号检索"),
    status: str | None = Query(default=None, description="进行中、已提交、已回滚"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """归档窗口分页列表，检查点数量、大小、来源编号直接随窗口返回。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = log_archive_service.list_windows(
        keyword=keyword, status=status, page=page, size=size
    )
    return PageResult(items=items, total=total, page=page, size=size)


@router.post("/windows", response_model=ActionResult)
def create_window(payload: ArchiveRequest) -> ActionResult:
    """按归档窗口执行完整链路；故障注入导致的中断会回滚并保留断点。"""
    try:
        result = log_archive_service.archive(
            window_id=payload.window_id,
            source_number=payload.source_number,
            simulate_failure=payload.simulate_failure,
        )
    except ArchiveConflictError as exc:
        raise _fail(exc, status=409)
    except ArchiveVerifyError as exc:
        raise _fail(exc)
    except RuntimeError as exc:
        raise _fail(exc, status=409)
    window = result["window"]
    if result.get("deduplicated"):
        message = "重复归档已拦截，归档清单未新增记录"
    elif window.get("rolled_back_count"):
        message = "断点重做完成，数据集、数量校验与审计记录已同时提交"
    else:
        message = "归档完成：数据集、记录数量校验与审计记录已同时提交"
    return ActionResult(ok=True, message=message, entry=result)


@router.get("/windows/{window_id}", response_model=dict)
def get_window(window_id: str) -> dict[str, Any]:
    window = log_archive_service.get_window(window_id)
    if window is None:
        raise HTTPException(status_code=404, detail=f"归档窗口 {window_id} 不存在")
    return window


@router.get("/windows/{window_id}/verify", response_model=dict)
def verify_window(window_id: str) -> dict[str, Any]:
    """检查点复核：记录数量、来源编号、哈希、审计记录与契约快照逐项对账。"""
    try:
        return log_archive_service.verify_window(window_id)
    except ArchiveVerifyError as exc:
        raise _fail(exc, status=409)


@router.get("/windows/{window_id}/manifest", response_model=PageResult[dict])
def list_manifest(window_id: str) -> PageResult[dict]:
    """调用日志归档数据集明细，即归档清单本身。"""
    if log_archive_service.get_window(window_id) is None:
        raise HTTPException(status_code=404, detail=f"归档窗口 {window_id} 不存在")
    items = log_archive_service.list_manifest(window_id)
    return PageResult(items=items, total=len(items), page=1, size=len(items) or 1)


@router.get("/windows/{window_id}/audits", response_model=PageResult[dict])
def list_audits(window_id: str) -> PageResult[dict]:
    """窗口下的审计记录，包含回滚与重复拦截记录，全部可复核。"""
    items = log_archive_service.list_audits(window_id)
    return PageResult(items=items, total=len(items), page=1, size=len(items) or 1)


@router.get("/windows/{window_id}/txns", response_model=PageResult[dict])
def list_txns(window_id: str) -> PageResult[dict]:
    """事务与断点记录：中断、回滚、重做的全过程都在这里。"""
    items = log_archive_service.list_txns(window_id)
    return PageResult(items=items, total=len(items), page=1, size=len(items) or 1)


@router.get("/windows/{window_id}/export/{fmt}")
def export_manifest(window_id: str, fmt: str) -> Response:
    """导出/下载清单文件（jsonl 或 json），缺行时拒绝导出。"""
    try:
        filename, media, content = log_archive_service.export_payload(window_id, fmt=fmt)
    except ArchiveVerifyError as exc:
        raise _fail(exc, status=409)
    return Response(
        content=content,
        media_type=media,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/windows/{window_id}/package")
def download_package(window_id: str) -> Response:
    """备份打包下载：清单、审计、契约、校验说明同包交付。"""
    try:
        filename, content = log_archive_service.package_payload(window_id)
    except ArchiveVerifyError as exc:
        raise _fail(exc, status=409)
    return Response(
        content=content,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/contracts", response_model=PageResult[dict])
def list_contracts(
    window_id: str | None = Query(default=None, description="按归档窗口过滤契约"),
) -> PageResult[dict]:
    """日志归档契约：备份模块读取的同一数据快照。"""
    items = log_archive_service.list_contracts(window_id)
    return PageResult(items=items, total=len(items), page=1, size=len(items) or 1)
