"""调用日志归档链路测试：覆盖数量/大小/来源编号、幂等去重、冲突优先级、
事务回滚断点重做、审计可复核、契约同快照与导出不缺行。

运行：cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import io
import json
import unittest
import zipfile

from app.services.log_archive import (
    ARCHIVE_MODULE,
    AUDIT_MODULE,
    CONTRACT_MODULE,
    TXN_MODULE,
    WINDOW_COMMITTED,
    WINDOW_ROLLED_BACK,
    ArchiveConflictError,
    _record_key,
    _sha256,
    log_archive_service as svc,
)
from app.store import store


class LogArchiveTestCase(unittest.TestCase):
    def setUp(self) -> None:
        store.reset()

    # ---- 用例 1：LOG-0003 三条日志复测，数量/大小/来源编号一致 ----
    def test_log0003_archive_count_size_sources(self) -> None:
        result = svc.archive(window_id="WIN-T1", source_number="LOG-0003")
        window, manifest = result["window"], result["manifest"]

        self.assertEqual(window["status"], WINDOW_COMMITTED)
        self.assertEqual(len(manifest), 3)
        self.assertEqual(window["record_count"], 3)
        self.assertEqual(window["source_numbers"], ["LOG-0003"])
        expect_size = sum(int(item["size_bytes"]) for item in manifest)
        self.assertEqual(window["size_bytes"], expect_size)
        # 检查点可从归档数据集重新复核
        verify = svc.verify_window("WIN-T1")
        self.assertTrue(verify["ok"], verify["checks"])
        self.assertTrue(verify["checks"]["记录数量"])
        self.assertTrue(verify["checks"]["来源编号"])
        self.assertTrue(verify["checks"]["审计记录可复核"])

    # ---- 用例 2：重复归档不新增记录（幂等）----
    def test_duplicate_archive_adds_no_record(self) -> None:
        first = svc.archive(window_id="WIN-T2", source_number="LOG-0003")
        ids_before = [item["id"] for item in svc.list_manifest("WIN-T2")]
        size_before = first["window"]["size_bytes"]
        hash_before = first["window"]["manifest_hash"]

        again = svc.archive(window_id="WIN-T2", source_number="LOG-0003")
        ids_after = [item["id"] for item in svc.list_manifest("WIN-T2")]

        self.assertTrue(again.get("deduplicated"))
        self.assertEqual(ids_before, ids_after)
        self.assertEqual(len(svc.list_manifest("WIN-T2")), 3)
        self.assertEqual(again["window"]["size_bytes"], size_before)
        self.assertEqual(again["window"]["manifest_hash"], hash_before)
        self.assertTrue(
            any(row["result"] == "重复归档已拦截" for row in svc.list_audits("WIN-T2"))
        )

    # ---- 用例 3：同窗口相同调用日志重复到达，清单只保留一份记录和来源版本 ----
    def test_same_call_log_dedup_keeps_single_record_and_version(self) -> None:
        rows = store.rows("log")
        target = next(row for row in rows if row["id"] == 5)
        # 完全相同的调用日志（同来源编号/请求时间/调用服务）再到达一次
        clone = dict(target)
        clone["id"] = max(int(row["id"]) for row in rows) + 1
        rows.append(clone)

        result = svc.archive(window_id="WIN-T3", source_number="LOG-0003")
        manifest = result["manifest"]
        keys = [item["record_key"] for item in manifest]
        self.assertEqual(len(keys), len(set(keys)))
        self.assertEqual(len(manifest), 3)
        dup_key = _record_key(target)
        kept = [item for item in manifest if item["record_key"] == dup_key]
        self.assertEqual(len(kept), 1)
        self.assertEqual(kept[0]["来源版本"], "v2")

    # ---- 用例 4：冲突优先级——高版本覆盖低版本，同版本按状态优先级 ----
    def test_conflict_priority_version_then_status(self) -> None:
        rows = store.rows("log")
        base = next(row for row in rows if row["id"] == 3)
        # 同调用键、更高来源版本
        newer = dict(base)
        newer.update({"id": 101, "来源版本": "v9", "status": "草稿"})
        # 同调用键、同 v1 但状态为已清理（低优先级）
        lower = dict(base)
        lower.update({"id": 102, "来源版本": "v1", "status": "已清理"})
        rows.extend([newer, lower])

        result = svc.archive(window_id="WIN-T4", source_number="LOG-0003")
        key = _record_key(base)
        kept = next(item for item in result["manifest"] if item["record_key"] == key)
        self.assertEqual(kept["来源版本"], "v9")
        self.assertEqual(kept["source_status"], "草稿")
        conflicts = result["audit"]["conflicts"]
        self.assertTrue(any("v9" in note["reason"] or "新版本覆盖" in note["reason"] for note in conflicts))

        # 再来一个 v9 且状态为已归档（同版本下状态优先级更高）应覆盖
        high_status = dict(newer)
        high_status.update({"id": 103, "status": "已归档"})
        rows.append(high_status)
        result2 = svc.archive(window_id="WIN-T4B", source_number="LOG-0003")
        kept2 = next(item for item in result2["manifest"] if item["record_key"] == key)
        self.assertEqual(kept2["来源版本"], "v9")
        self.assertEqual(kept2["source_status"], "已归档")

    # ---- 用例 5：事务提交中断回滚，未完成记录不留残，再从断点重做 ----
    def test_rollback_then_resume_from_breakpoint(self) -> None:
        with self.assertRaises(RuntimeError):
            svc.archive(window_id="WIN-T5", source_number="LOG-0003", simulate_failure="commit")

        window = svc.get_window("WIN-T5")
        self.assertEqual(window["status"], WINDOW_ROLLED_BACK)
        self.assertEqual(svc.list_manifest("WIN-T5"), [])
        self.assertEqual(svc.list_contracts("WIN-T5"), [])
        self.assertTrue(svc.list_audits("WIN-T5")[-1]["result"] == "已回滚")
        txn = svc.list_txns("WIN-T5")[-1]
        self.assertEqual(txn["status"], WINDOW_ROLLED_BACK)
        self.assertIn("processed", txn["checkpoint"])

        # 同一窗口编号从断点重做
        redo = svc.archive(window_id="WIN-T5", source_number="LOG-0003")
        self.assertEqual(redo["window"]["status"], WINDOW_COMMITTED)
        self.assertEqual(redo["window"]["rolled_back_count"], 1)
        self.assertEqual(len(redo["manifest"]), 3)
        self.assertTrue(svc.verify_window("WIN-T5")["ok"])
        # 数据集里该窗口没有重复/残留行
        self.assertEqual(len(svc.list_manifest("WIN-T5")), 3)

    # ---- 用例 6：暂存/校验阶段中断同样回滚 ----
    def test_stage_and_verify_failure_rollback(self) -> None:
        for phase, win in (("stage", "WIN-T6A"), ("verify", "WIN-T6B")):
            with self.subTest(phase=phase):
                with self.assertRaises(RuntimeError):
                    svc.archive(window_id=win, source_number="LOG-0003", simulate_failure=phase)
                self.assertEqual(svc.get_window(win)["status"], WINDOW_ROLLED_BACK)
                self.assertEqual(svc.list_manifest(win), [])

    # ---- 用例 7：数据集、数量校验、审计记录与契约同时提交 ----
    def test_atomic_commit_dataset_count_audit_contract(self) -> None:
        result = svc.archive(window_id="WIN-T7")
        self.assertIsNotNone(result["audit"])
        self.assertIsNotNone(result["contract"])
        audit = result["audit"]
        contract = result["contract"]
        window = result["window"]
        # 四项检查点互相咬合
        self.assertEqual(audit["record_count"], window["record_count"])
        self.assertEqual(audit["manifest_hash"], window["manifest_hash"])
        self.assertEqual(contract["snapshot_hash"], audit["manifest_hash"])
        self.assertEqual(contract["record_count"], audit["record_count"])
        self.assertEqual(len(contract["manifest"]), window["record_count"])
        self.assertIn("backup", contract["consumed_by"])

    # ---- 用例 8：备份模块通过日志归档契约读到同一数据快照 ----
    def test_backup_consumes_same_snapshot(self) -> None:
        svc.archive(window_id="WIN-T8", source_number="LOG-0003")
        contracts = svc.list_contracts("WIN-T8")
        self.assertEqual(len(contracts), 1)
        manifest = svc.list_manifest("WIN-T8")
        self.assertEqual(
            [item["sha256"] for item in contracts[0]["manifest"]],
            [item["sha256"] for item in manifest],
        )
        self.assertEqual(contracts[0]["snapshot_hash"], svc.get_window("WIN-T8")["manifest_hash"])

    # ---- 用例 9：导出文件/下载/备份打包，清单不缺行 ----
    def test_export_and_package_no_missing_rows(self) -> None:
        svc.archive(window_id="WIN-T9", source_number="LOG-0003")

        name, media, raw = svc.export_payload("WIN-T9", fmt="jsonl")
        self.assertTrue(name.endswith(".jsonl"))
        lines = [line for line in raw.decode("utf-8").splitlines() if line.strip()]
        self.assertEqual(len(lines), 3)
        for line in lines:
            json.loads(line)

        _, _, json_raw = svc.export_payload("WIN-T9", fmt="json")
        payload = json.loads(json_raw.decode("utf-8"))
        self.assertEqual(len(payload["items"]), 3)
        self.assertEqual(payload["checkpoint"]["record_count"], 3)

        pkg_name, pkg_raw = svc.package_payload("WIN-T9")
        self.assertTrue(pkg_name.endswith(".zip"))
        with zipfile.ZipFile(io.BytesIO(pkg_raw)) as zf:
            names = set(zf.namelist())
            self.assertIn("WIN-T9-manifest.jsonl", names)
            self.assertIn("WIN-T9-audit.json", names)
            self.assertIn("WIN-T9-contract.json", names)
            self.assertIn("WIN-T9-checksum.txt", names)
            packed_lines = [
                line for line in zf.read("WIN-T9-manifest.jsonl").decode("utf-8").splitlines()
                if line.strip()
            ]
            self.assertEqual(len(packed_lines), 3)

    # ---- 用例 10：已提交窗口快照不一致时冲突报错，禁止覆盖 ----
    def test_committed_window_conflict_raises(self) -> None:
        svc.archive(window_id="WIN-T10", source_number="LOG-0003")
        store.rows("log").append({
            "id": 999,
            "status": "草稿",
            "记录编号": "LOG-0003",
            "来源编号": "LOG-0003",
            "来源版本": "v3",
            "调用服务": "新增冲突服务",
            "请求时间": "2026-09-30",
        })
        with self.assertRaises(ArchiveConflictError):
            svc.archive(window_id="WIN-T10", source_number="LOG-0003")
        # 原清单不受影响
        self.assertEqual(len(svc.list_manifest("WIN-T10")), 3)

    # ---- 用例 11：逐条哈希可独立复核，数据集行被篡改能被发现 ----
    def test_tampered_record_fails_verification(self) -> None:
        svc.archive(window_id="WIN-T11", source_number="LOG-0003")
        row = svc.list_manifest("WIN-T11")[0]
        row["snapshot"]["调用服务"] = "被篡改的服务名"
        verify = svc.verify_window("WIN-T11")
        self.assertFalse(verify["ok"])
        self.assertFalse(verify["checks"]["逐条哈希完整"])

    # ---- 用例 12：全量归档与分页/统计口径 ----
    def test_full_archive_and_stats(self) -> None:
        result = svc.archive(window_id="WIN-T12")
        total_keys = len({_record_key(row) for row in store.rows("log")})
        self.assertEqual(result["window"]["record_count"], total_keys)
        stats = svc.stats()
        self.assertEqual(stats["window_committed"], 1)
        self.assertEqual(stats["dataset_records"], total_keys)
        self.assertEqual(stats["audit_records"], 1)
        self.assertEqual(stats["contract_records"], 1)
        items, total = svc.list_windows(page=1, size=10)
        self.assertEqual(total, 1)
        self.assertEqual(items[0]["window_id"], "WIN-T12")
        # 哈希工具确定性
        self.assertEqual(_sha256({"a": 1}), _sha256({"a": 1}))
        self.assertEqual(len(store.rows(TXN_MODULE)), 1)


if __name__ == "__main__":
    unittest.main()
