"""基线监控管理接口：维护基线配置，覆盖启用监控、调整基线、关闭基线等动作。

时间推演相关接口：
- POST /api/baseline/derive          推演全部启用基线，或用 baseline_id / scope_ref 推演单条
- POST /api/baseline/{id}/actions    执行动作后回执同步到 DAG 节点清单并重算 SLA 风险
- GET  /api/baseline/snapshots       按时间倒序查看历史推演快照
- GET  /api/baseline/derivation/chain 查看 DAG → 基线 → SLA 的调用链与取数环节
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import ActionResult, EntryPayload, PageResult
from app.services.baseline import BaselineService
from app.services.baseline_time import derivation_chain, derivation_service

router = APIRouter(prefix="/api/baseline", tags=["基线监控管理"])

service = BaselineService()

LIST_FIELDS = ["基线编号", "基线名称", "承诺时间", "预警时间", "关键路径", "依赖范围", "监控状态", "最近产出"]
STATUSES = ["草稿", "监控中", "已延期", "已关闭"]


@router.get("", response_model=PageResult[dict])
def list_entries(
    keyword: str | None = Query(default=None, description="按基线编号检索"),
    status: str | None = Query(default=None, description="草稿、监控中、已延期、已关闭"),
    page: int = 1,
    size: int = 20,
) -> PageResult[dict]:
    """按基线编号与状态过滤基线监控管理列表；没有数据时返回空页，不报错。"""
    if size > 200:
        raise HTTPException(status_code=400, detail="每页最多 200 条，请缩小分页范围")
    items, total = service.list_entries(keyword=keyword, status=status, page=page, size=size)
    return PageResult(items=items, total=total, page=page, size=size)


@router.get("/snapshots", response_model=dict)
def list_snapshots(
    baseline: str | None = Query(default=None, description="按基线编号过滤"),
    limit: int = Query(default=20, ge=1, le=100),
) -> dict[str, Any]:
    """查看历史推演快照，按生成时间倒序；同一版本只保留一份。"""
    rows = derivation_service.snapshots(baseline=baseline, limit=limit)
    return {"total": len(rows), "items": rows}


@router.get("/snapshots/{snapshot_id}", response_model=dict)
def get_snapshot(snapshot_id: str) -> dict[str, Any]:
    """读取单份快照的完整推演过程，用于追溯时间口径与取数。"""
    snapshot = derivation_service.get_snapshot(snapshot_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail=f"推演快照 {snapshot_id} 不存在")
    return snapshot


@router.get("/derivation/chain", response_model=dict)
def get_derivation_chain() -> dict[str, Any]:
    """DAG → 基线 → SLA 的调用链：经过的节点、取数环节与不可推演判据。"""
    return derivation_chain()


@router.post("/derive", response_model=ActionResult)
def derive(
    payload: EntryPayload,
) -> ActionResult:
    """按依赖范围、关键路径、节点超时与最近产出推演基线时间。

    values 支持：
    - baseline_id / baseline_ref：只推演指定基线；
    - scope_ref / scope_id：把某个 DAG 节点及其全部上游当作临时基线推演；
    - dry_run：只预演不落快照、不写回台账。

    循环依赖、节点缺失、超时缺失等一律返回不可推演原因，不会按 0 处理。
    """
    values = payload.merged()
    baseline_id = _as_int(values.get("baseline_id"))
    scope_id = _as_int(values.get("scope_id"))
    dry_run = bool(values.get("dry_run"))
    snapshot, message = derivation_service.derive(
        baseline_id=baseline_id,
        baseline_ref=_as_text(values.get("baseline_ref")),
        scope_id=scope_id,
        scope_ref=_as_text(values.get("scope_ref")),
        now=_as_text(values.get("now")),
        dry_run=dry_run,
    )
    if snapshot is None:
        return ActionResult(ok=False, message=message)
    return ActionResult(ok=True, message=message, entry=snapshot)


@router.get("/export")
def export_entries() -> dict[str, Any]:
    """导出基线监控管理清单：返回当前过滤条件下的全量数据。"""
    items, total = service.list_entries(page=1, size=10000)
    return {"module": "baseline", "total": total, "items": items}


@router.get("/{entry_id}", response_model=dict)
def get_entry(entry_id: int) -> dict:
    """读取单条基线配置明细；不存在时给出可读的错误说明。"""
    entry = service.get_entry(entry_id)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"基线配置 {entry_id} 不存在或已归档")
    return entry


@router.post("", response_model=ActionResult)
def create_entry(payload: EntryPayload) -> ActionResult:
    """登记一条基线配置，缺字段时说明原因而不是静默丢弃。"""
    entry, missing = service.create_entry(payload.merged())
    if missing:
        return ActionResult(ok=False, message=f"缺少必填字段：{'、'.join(missing)}")
    return ActionResult(ok=True, message="基线配置已登记", entry=entry)


@router.post("/{entry_id}/actions", response_model=ActionResult)
def run_action(entry_id: int, payload: EntryPayload) -> ActionResult:
    """对单条基线配置执行启用监控、调整基线、关闭基线；不允许的动作会被拦下并说明原因。

    动作成功后：回执同步到该基线范围内的 DAG 节点，并重算关联 SLA 规则的风险字段。
    """
    action = str(payload.merged().get("action") or "").strip()
    entry, message = service.run_action(entry_id, action)
    if entry is None:
        return ActionResult(ok=False, message=message)
    synced = derivation_service.sync_action_receipt(entry, action, message)
    snapshot, summary = derivation_service.derive(baseline_id=entry_id)
    entry["同步节点"] = synced
    entry["推演快照"] = snapshot["snapshot_id"] if snapshot else None
    entry["SLA重算"] = summary if snapshot else None
    return ActionResult(
        ok=True,
        message=f"{message}；已同步 {len(synced)} 个 DAG 节点回执，SLA 风险已重算",
        entry=entry,
    )


def _as_int(value: Any) -> int | None:
    if value is None or str(value).strip() == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _as_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
