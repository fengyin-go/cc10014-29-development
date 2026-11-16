"""调用日志归档处理链路。

链路：标准化日志记录（store 的 log 表）→ 按归档窗口写入归档数据集（log_archive）
→ 记录数量/来源编号/审计记录三项检查点校验 → 审计记录（log_archive_audit）与
日志归档契约（log_archive_contract，供备份模块消费同一份快照）在同一事务里提交。

约束：
- 归档数据集、记录数量校验、审计记录必须同时落库，任一环节失败就整体回滚，
  事务表（log_archive_txn）记录断点，之后可用同一窗口编号从断点重做。
- 同一归档窗口内，相同调用日志（来源编号 + 请求时间 + 调用服务相同）重复到达时，
  清单只保留一份记录与来源版本；冲突按“来源版本 → 状态优先级 → 到达顺序”判定。
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Any

from app.store import store

# 标准化日志来源模块与归档链路的四张表
SOURCE_MODULE = "log"
WINDOW_MODULE = "log_archive_window"
ARCHIVE_MODULE = "log_archive"
AUDIT_MODULE = "log_archive_audit"
CONTRACT_MODULE = "log_archive_contract"
TXN_MODULE = "log_archive_txn"

# 同版本冲突时的状态优先级：数值越大越优先
STATUS_PRIORITY = {"已归档": 4, "记录中": 3, "草稿": 2, "已清理": 1}

WINDOW_COMMITTED = "已提交"
WINDOW_RUNNING = "进行中"
WINDOW_ROLLED_BACK = "已回滚"

# 进入归档数据集的标准化字段（与调用日志模块的清单口径保持一致）
SNAPSHOT_FIELDS = [
    "记录编号", "调用服务", "请求时间", "响应耗时", "返回码",
    "调用方IP", "请求大小KB", "响应大小KB",
]


class ArchiveConflictError(Exception):
    """窗口已提交且新快照与已归档快照不一致时抛出，由路由层转成 409。"""


class ArchiveVerifyError(Exception):
    """检查点复核失败：数据集与清单对不上，事务必须回滚。"""


def _now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _canonical(payload: Any) -> str:
    """确定性 JSON 序列化：保证同内容永远得到同一份字节与哈希。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256(payload: Any) -> str:
    return hashlib.sha256(_canonical(payload).encode("utf-8")).hexdigest()


def _source_number(row: dict[str, Any]) -> str:
    """来源编号优先取标准化记录自带字段，缺省回退到记录编号。"""
    return str(row.get("来源编号") or row.get("记录编号") or "").strip()


def _source_version(row: dict[str, Any]) -> str:
    return str(row.get("来源版本") or "v1").strip()


def _version_key(version: str) -> tuple[Any, ...]:
    """v2、v10 这类版本号按数值比较，而不是字典序。"""
    text = version.lstrip("vV")
    parts: list[Any] = []
    for piece in text.split("."):
        parts.append(int(piece) if piece.isdigit() else piece)
    return tuple(parts)


def _record_key(row: dict[str, Any]) -> str:
    """相同调用日志的判定键：来源编号 + 请求时间 + 调用服务。"""
    return "|".join([
        _source_number(row),
        str(row.get("请求时间") or ""),
        str(row.get("调用服务") or ""),
    ])


def _snapshot(row: dict[str, Any]) -> dict[str, Any]:
    return {field: row.get(field) for field in SNAPSHOT_FIELDS}


def _next_id(module: str) -> int:
    return max((int(item.get("id", 0)) for item in store.rows(module)), default=0) + 1


def _manifest_entry(row: dict[str, Any], window_id: str) -> dict[str, Any]:
    """把一条标准化日志整理成归档数据集明细记录。"""
    snapshot = _snapshot(row)
    source_no = _source_number(row)
    source_ver = _source_version(row)
    body = {
        "window_id": window_id,
        "record_key": _record_key(row),
        "来源编号": source_no,
        "来源版本": source_ver,
        "记录编号": str(row.get("记录编号") or ""),
        "调用服务": str(row.get("调用服务") or ""),
        "请求时间": str(row.get("请求时间") or ""),
        "source_status": str(row.get("status") or ""),
        "source_id": int(row.get("id", 0)),
        "snapshot": snapshot,
    }
    body["sha256"] = _sha256(body)
    body["size_bytes"] = len(_canonical(body).encode("utf-8"))
    return body


def _pick_winner(
    current: dict[str, Any],
    candidate: dict[str, Any],
) -> tuple[dict[str, Any], str]:
    """冲突优先级判定：来源版本高者胜 → 状态优先级高者胜 → 后到达者胜。"""
    cur_ver, new_ver = _version_key(current["来源版本"]), _version_key(candidate["来源版本"])
    if new_ver > cur_ver:
        return candidate, f"来源版本 {candidate['来源版本']} 高于 {current['来源版本']}，新版本覆盖"
    if new_ver < cur_ver:
        return current, f"来源版本 {current['来源版本']} 更高，保留已归档版本"
    cur_pri = STATUS_PRIORITY.get(current["source_status"], 0)
    new_pri = STATUS_PRIORITY.get(candidate["source_status"], 0)
    if new_pri > cur_pri:
        return candidate, f"同版本下状态 {candidate['source_status']} 优先级高于 {current['source_status']}"
    if new_pri < cur_pri:
        return current, f"同版本下状态 {current['source_status']} 优先级更高，保留原记录"
    if int(candidate.get("source_id", 0)) >= int(current.get("source_id", 0)):
        return candidate, "版本与状态均相同，后到达记录覆盖先到达记录"
    return current, "版本与状态均相同，保留先到达记录"


class LogArchiveService:
    """归档窗口、数据集、检查点、审计与契约的统一业务入口。"""

    # ---------------- 查询类 ----------------

    def list_windows(
        self,
        *,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        rows = list(store.rows(WINDOW_MODULE))
        if keyword:
            rows = [row for row in rows if keyword in str(row.get("window_id", ""))]
        if status:
            rows = [row for row in rows if row.get("status") == status]
        rows.sort(key=lambda item: int(item.get("id", 0)), reverse=True)
        total = len(rows)
        start = max(page - 1, 0) * size
        return rows[start:start + size], total

    def get_window(self, window_id: str) -> dict[str, Any] | None:
        for row in store.rows(WINDOW_MODULE):
            if row.get("window_id") == window_id:
                return row
        return None

    def list_manifest(self, window_id: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(ARCHIVE_MODULE)
        if window_id:
            rows = [row for row in rows if row.get("window_id") == window_id]
        return sorted(rows, key=lambda item: (str(item.get("window_id")), str(item.get("record_key"))))

    def list_audits(self, window_id: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(AUDIT_MODULE)
        if window_id:
            rows = [row for row in rows if row.get("window_id") == window_id]
        return sorted(rows, key=lambda item: int(item.get("id", 0)))

    def list_contracts(self, window_id: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(CONTRACT_MODULE)
        if window_id:
            rows = [row for row in rows if row.get("window_id") == window_id]
        return sorted(rows, key=lambda item: int(item.get("id", 0)), reverse=True)

    def list_txns(self, window_id: str | None = None) -> list[dict[str, Any]]:
        rows = store.rows(TXN_MODULE)
        if window_id:
            rows = [row for row in rows if row.get("window_id") == window_id]
        return sorted(rows, key=lambda item: int(item.get("id", 0)))

    def stats(self) -> dict[str, int]:
        committed = [row for row in store.rows(WINDOW_MODULE) if row.get("status") == WINDOW_COMMITTED]
        return {
            "window_total": len(store.rows(WINDOW_MODULE)),
            "window_committed": len(committed),
            "dataset_records": len(store.rows(ARCHIVE_MODULE)),
            "audit_records": len(store.rows(AUDIT_MODULE)),
            "contract_records": len(store.rows(CONTRACT_MODULE)),
            "rolled_back": len([
                row for row in store.rows(WINDOW_MODULE)
                if row.get("status") == WINDOW_ROLLED_BACK
            ]),
        }

    # ---------------- 核心链路 ----------------

    def archive(
        self,
        *,
        window_id: str | None = None,
        source_number: str | None = None,
        simulate_failure: str | None = None,
    ) -> dict[str, Any]:
        """按归档窗口把标准化日志写入数据集，校验通过后与审计、契约同事务提交。

        simulate_failure 仅用于演练回滚：``stage``/``commit``/``verify``
        分别在暂存、提交、校验阶段制造一次中断。
        """
        window_id = (window_id or f"WIN-{datetime.now().strftime('%Y%m%d%H%M%S')}").strip()
        existing = self.get_window(window_id)
        if existing and existing.get("status") == WINDOW_COMMITTED:
            return self._reuse_committed_window(existing, source_number)

        # 已回滚窗口：清理残留断点数据后从断点重做
        if existing and existing.get("status") == WINDOW_ROLLED_BACK:
            self._purge_window(window_id)
            window = existing
            window["status"] = WINDOW_RUNNING
            window["rolled_back_count"] = int(window.get("rolled_back_count", 0)) + 1
        else:
            window = {
                "id": _next_id(WINDOW_MODULE),
                "window_id": window_id,
                "status": WINDOW_RUNNING,
                "source_filter": {"source_number": source_number},
                "record_count": 0,
                "size_bytes": 0,
                "source_numbers": [],
                "manifest_hash": "",
                "rolled_back_count": 0,
                "created_at": _now(),
                "committed_at": None,
                "checkpoint": None,
            }
            store.rows(WINDOW_MODULE).append(window)

        txn = {
            "id": _next_id(TXN_MODULE),
            "window_id": window_id,
            "status": "进行中",
            "phase": "准备",
            "checkpoint": {"processed": 0, "staged_keys": []},
            "simulate_failure": simulate_failure,
            "created_at": _now(),
            "finished_at": None,
            "message": "",
        }
        store.rows(TXN_MODULE).append(txn)

        try:
            staged, conflict_notes = self._stage(window_id, source_number, txn, simulate_failure)
            if simulate_failure == "commit":
                raise RuntimeError("模拟事务提交中断：提交前故障注入")
            checkpoint = self._verify_staging(window_id, staged, simulate_failure)
            self._commit(window, staged, checkpoint, conflict_notes, txn)
        except Exception as exc:  # 任何中断都回滚未完成记录，保留断点
            self._rollback(window, txn, exc)
            raise

        return {
            "window": window,
            "manifest": staged,
            "audit": self.list_audits(window_id)[-1],
            "contract": self.list_contracts(window_id)[-1],
        }

    def _stage(
        self,
        window_id: str,
        source_number: str | None,
        txn: dict[str, Any],
        simulate_failure: str | None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
        """从标准化日志取数，按冲突优先级把每条调用日志在窗口内只保留一份。"""
        source_rows = store.rows(SOURCE_MODULE)
        if source_number:
            source_rows = [row for row in source_rows if _source_number(row) == source_number]

        staged: dict[str, dict[str, Any]] = {}
        conflict_notes: list[dict[str, str]] = []
        processed = 0
        for row in source_rows:
            candidate = _manifest_entry(row, window_id)
            processed += 1
            txn["phase"] = "暂存"
            txn["checkpoint"] = {
                "processed": processed,
                "staged_keys": sorted(staged.keys()),
                "current_key": candidate["record_key"],
            }
            if simulate_failure == "stage" and processed == max(1, len(source_rows)):
                raise RuntimeError("模拟事务提交中断：暂存阶段故障注入")
            current = staged.get(candidate["record_key"])
            if current is None:
                staged[candidate["record_key"]] = candidate
                continue
            winner, reason = _pick_winner(current, candidate)
            conflict_notes.append({
                "record_key": candidate["record_key"],
                "来源编号": candidate["来源编号"],
                "kept_version": winner["来源版本"],
                "dropped_version": (
                    candidate["来源版本"] if winner is current else current["来源版本"]
                ),
                "reason": reason,
            })
            staged[candidate["record_key"]] = winner
        return list(staged.values()), conflict_notes

    def _verify_staging(
        self,
        window_id: str,
        staged: list[dict[str, Any]],
        simulate_failure: str | None,
    ) -> dict[str, Any]:
        """三项检查点：记录数量、来源编号、可复核哈希/大小。"""
        if simulate_failure == "verify":
            raise RuntimeError("模拟事务提交中断：完整性校验故障注入")
        ordered = sorted(staged, key=lambda item: item["record_key"])
        record_count = len(ordered)
        source_numbers = sorted({item["来源编号"] for item in ordered})
        size_bytes = sum(int(item["size_bytes"]) for item in ordered)
        manifest_hash = hashlib.sha256(
            "\n".join(item["sha256"] for item in ordered).encode("utf-8")
        ).hexdigest()
        if record_count == 0:
            raise ArchiveVerifyError("标准化日志没有可取数记录，拒绝生成空归档窗口")
        return {
            "window_id": window_id,
            "record_count": record_count,
            "source_numbers": source_numbers,
            "size_bytes": size_bytes,
            "manifest_hash": manifest_hash,
            "record_keys": [item["record_key"] for item in ordered],
            "verified_at": _now(),
        }

    def _commit(
        self,
        window: dict[str, Any],
        staged: list[dict[str, Any]],
        checkpoint: dict[str, Any],
        conflict_notes: list[dict[str, str]],
        txn: dict[str, Any],
    ) -> None:
        """数据集明细、数量校验、审计记录与契约快照在同一事务里一次性提交。"""
        window_id = window["window_id"]
        ordered = sorted(staged, key=lambda item: item["record_key"])

        # 1) 归档数据集明细（带独立 id 落库）
        for item in ordered:
            item["id"] = _next_id(ARCHIVE_MODULE)
            item["archived_at"] = _now()
            store.rows(ARCHIVE_MODULE).append(item)

        # 2) 窗口检查点（记录数量 + 来源编号 + 哈希/大小）
        window["record_count"] = checkpoint["record_count"]
        window["size_bytes"] = checkpoint["size_bytes"]
        window["source_numbers"] = checkpoint["source_numbers"]
        window["manifest_hash"] = checkpoint["manifest_hash"]
        window["checkpoint"] = checkpoint
        window["status"] = WINDOW_COMMITTED
        window["committed_at"] = _now()

        # 3) 审计记录（与数据集同时提交，缺它就无法证明没有漏行）
        audit = {
            "id": _next_id(AUDIT_MODULE),
            "window_id": window_id,
            "audit_no": f"AUD-{window_id}",
            "record_count": checkpoint["record_count"],
            "size_bytes": checkpoint["size_bytes"],
            "source_numbers": checkpoint["source_numbers"],
            "manifest_hash": checkpoint["manifest_hash"],
            "conflict_count": len(conflict_notes),
            "conflicts": conflict_notes,
            "result": "通过",
            "created_at": _now(),
        }
        store.rows(AUDIT_MODULE).append(audit)

        # 4) 日志归档契约：备份模块读这同一份数据快照
        contract = {
            "id": _next_id(CONTRACT_MODULE),
            "window_id": window_id,
            "contract_no": f"CONT-{window_id}",
            "snapshot_hash": checkpoint["manifest_hash"],
            "record_count": checkpoint["record_count"],
            "size_bytes": checkpoint["size_bytes"],
            "source_numbers": checkpoint["source_numbers"],
            "manifest": [dict(item) for item in ordered],
            "consumed_by": ["backup"],
            "published_at": _now(),
        }
        store.rows(CONTRACT_MODULE).append(contract)

        txn["status"] = "已提交"
        txn["phase"] = "完成"
        txn["checkpoint"] = {
            "processed": checkpoint["record_count"],
            "staged_keys": checkpoint["record_keys"],
        }
        txn["finished_at"] = _now()
        txn["message"] = "数据集、数量校验与审计记录已同时提交"

    def _rollback(self, window: dict[str, Any], txn: dict[str, Any], exc: Exception) -> None:
        """回滚该窗口未完成的暂存记录，保留断点信息供重做。"""
        window_id = window["window_id"]
        self._purge_window(window_id)
        window["status"] = WINDOW_ROLLED_BACK
        window["checkpoint"] = txn.get("checkpoint")
        txn["status"] = WINDOW_ROLLED_BACK
        txn["finished_at"] = _now()
        txn["message"] = f"事务中断已回滚：{exc}；可使用同一窗口编号从断点重做"
        audit = {
            "id": _next_id(AUDIT_MODULE),
            "window_id": window_id,
            "audit_no": f"AUD-{window_id}-RB",
            "record_count": 0,
            "size_bytes": 0,
            "source_numbers": [],
            "manifest_hash": "",
            "conflict_count": 0,
            "conflicts": [],
            "result": "已回滚",
            "created_at": _now(),
            "rollback_reason": str(exc),
            "breakpoint": txn.get("checkpoint"),
        }
        store.rows(AUDIT_MODULE).append(audit)

    def _purge_window(self, window_id: str) -> None:
        for module in (ARCHIVE_MODULE, CONTRACT_MODULE):
            table = store.rows(module)
            table[:] = [row for row in table if row.get("window_id") != window_id]

    def _reuse_committed_window(
        self,
        window: dict[str, Any],
        source_number: str | None,
    ) -> dict[str, Any]:
        """重复归档：快照一致则幂等返回，绝不新增记录；不一致则冲突报错。"""
        candidate_rows = store.rows(SOURCE_MODULE)
        if source_number:
            candidate_rows = [
                row for row in candidate_rows if _source_number(row) == source_number
            ]
        candidate_keys = sorted({_record_key(row) for row in candidate_rows})
        committed_keys = list(window["checkpoint"]["record_keys"])
        if candidate_keys != committed_keys:
            raise ArchiveConflictError(
                f"窗口 {window['window_id']} 已提交且快照与当前标准化日志不一致，"
                "禁止覆盖已归档清单，请使用新窗口编号"
            )
        audit = {
            "id": _next_id(AUDIT_MODULE),
            "window_id": window["window_id"],
            "audit_no": f"AUD-{window['window_id']}-DUP",
            "record_count": window["record_count"],
            "size_bytes": window["size_bytes"],
            "source_numbers": window["source_numbers"],
            "manifest_hash": window["manifest_hash"],
            "conflict_count": 0,
            "conflicts": [],
            "result": "重复归档已拦截",
            "created_at": _now(),
        }
        store.rows(AUDIT_MODULE).append(audit)
        return {
            "window": window,
            "manifest": self.list_manifest(window["window_id"]),
            "audit": audit,
            "contract": self.list_contracts(window["window_id"])[-1],
            "deduplicated": True,
        }

    # ---------------- 复核与导出 ----------------

    def verify_window(self, window_id: str) -> dict[str, Any]:
        """从调用日志归档数据集重新计算检查点，与窗口/审计记录逐项复核。"""
        window = self.get_window(window_id)
        if window is None:
            raise ArchiveVerifyError(f"归档窗口 {window_id} 不存在")
        if window.get("status") != WINDOW_COMMITTED:
            raise ArchiveVerifyError(f"窗口 {window_id} 状态为 {window.get('status')}，无法复核")
        rows = self.list_manifest(window_id)
        audits = [
            row for row in self.list_audits(window_id)
            if row.get("result") == "通过"
        ]
        contracts = self.list_contracts(window_id)
        recomputed = {
            "record_count": len(rows),
            "source_numbers": sorted({item["来源编号"] for item in rows}),
            "size_bytes": sum(int(item["size_bytes"]) for item in rows),
            "manifest_hash": hashlib.sha256(
                "\n".join(item["sha256"] for item in rows).encode("utf-8")
            ).hexdigest(),
        }
        checks = {
            "记录数量": recomputed["record_count"] == int(window["record_count"]),
            "来源编号": recomputed["source_numbers"] == list(window["source_numbers"]),
            "大小校验": recomputed["size_bytes"] == int(window["size_bytes"]),
            "哈希校验": recomputed["manifest_hash"] == window["manifest_hash"],
            "审计记录可复核": bool(audits) and audits[-1]["manifest_hash"] == recomputed["manifest_hash"],
            "契约快照一致": bool(contracts) and contracts[-1]["snapshot_hash"] == recomputed["manifest_hash"],
            "逐条哈希完整": all(item["sha256"] == _sha256({
                key: value for key, value in item.items()
                if key not in ("id", "archived_at", "sha256", "size_bytes")
            }) for item in rows),
        }
        ok = all(checks.values())
        return {
            "window_id": window_id,
            "ok": ok,
            "checks": checks,
            "checkpoint": window["checkpoint"],
            "recomputed": recomputed,
            "audit": audits[-1] if audits else None,
            "verified_at": _now(),
        }

    def export_payload(self, window_id: str, *, fmt: str) -> tuple[str, str, bytes]:
        """导出清单文件：行数必须等于检查点记录数量，缺行直接拒绝导出。"""
        verify = self.verify_window(window_id)
        rows = self.list_manifest(window_id)
        if len(rows) != verify["checkpoint"]["record_count"]:
            raise ArchiveVerifyError("导出清单行数与检查点不一致，存在缺行，已拒绝导出")
        if fmt == "jsonl":
            content = "\n".join(_canonical({
                key: value for key, value in item.items() if key != "id"
            }) for item in rows)
            media = "application/x-ndjson"
            filename = f"{window_id}-manifest.jsonl"
        elif fmt == "json":
            content = json.dumps(
                {"window_id": window_id, "checkpoint": verify["checkpoint"], "items": rows},
                ensure_ascii=False,
                indent=2,
            )
            media = "application/json"
            filename = f"{window_id}-manifest.json"
        else:
            raise ArchiveVerifyError(f"不支持的导出格式：{fmt}")
        return filename, media, content.encode("utf-8")

    def package_payload(self, window_id: str) -> tuple[str, bytes]:
        """备份打包：清单（JSONL/JSON）+ 审计 + 契约 + 校验说明放进同一个 zip。"""
        import io
        import zipfile

        verify = self.verify_window(window_id)
        _, _, jsonl_bytes = self.export_payload(window_id, fmt="jsonl")
        _, _, json_bytes = self.export_payload(window_id, fmt="json")
        audits = [row for row in self.list_audits(window_id) if row.get("result") == "通过"]
        contracts = self.list_contracts(window_id)
        checksum_text = (
            f"window_id={window_id}\n"
            f"record_count={verify['recomputed']['record_count']}\n"
            f"size_bytes={verify['recomputed']['size_bytes']}\n"
            f"manifest_hash={verify['recomputed']['manifest_hash']}\n"
            f"source_numbers={','.join(verify['recomputed']['source_numbers'])}\n"
        )
        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            archive.writestr(f"{window_id}-manifest.jsonl", jsonl_bytes)
            archive.writestr(f"{window_id}-manifest.json", json_bytes)
            archive.writestr(f"{window_id}-audit.json", _canonical(audits[-1] if audits else {}))
            archive.writestr(f"{window_id}-contract.json", _canonical(contracts[-1] if contracts else {}))
            archive.writestr(f"{window_id}-checksum.txt", checksum_text)
        return f"{window_id}-archive-package.zip", buffer.getvalue()


log_archive_service = LogArchiveService()
