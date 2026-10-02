import tempfile
import threading
import unittest
from datetime import date
from pathlib import Path

from app import Database, DomainError, seed_demo


class WaterRightsFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.db")
        self.accounts = seed_demo(self.db)

    def tearDown(self):
        self.tmp.cleanup()

    def test_transfer_approval_usage_and_drought(self):
        source, target = self.accounts["北区水库"], self.accounts["河口灌区"]
        transfer = self.db.create_transfer("alice", {"from_account_id": source, "to_account_id": target, "amount": 400, "effective_date": "2026-06-01"}, "editor")
        self.assertEqual(self.db.available(source)["reserved_outgoing"], 400)
        approved = self.db.approve_transfer(transfer["id"], "bob", "reviewer")
        self.assertEqual(approved["status"], "approved")
        usage = self.db.record_usage("meter-01", {"account_id": source, "amount": 150, "meter_event_id": "UP-JUL-1", "occurred_at": "2026-07-10"}, "meter")
        self.assertEqual(usage["amount"], 150)
        simulation = self.db.simulate_drought(1000, 0.3)
        self.assertAlmostEqual(sum(x["allocation"] for x in simulation["allocations"]) + simulation["unallocated"], 700)

    def test_duplicate_meter_event_and_pending_reservation(self):
        source, target = self.accounts["北区水库"], self.accounts["河口灌区"]
        self.db.create_transfer("alice", {"from_account_id": source, "to_account_id": target, "amount": 500, "effective_date": "2026-06-01"}, "editor")
        with self.assertRaises(DomainError):
            self.db.create_transfer("alice", {"from_account_id": source, "to_account_id": target, "amount": 1, "effective_date": "2026-06-01"}, "editor")
        self.db.record_usage("meter-01", {"account_id": source, "amount": 10, "meter_event_id": "M-1", "occurred_at": "2026-08-01"}, "meter")
        with self.assertRaisesRegex(DomainError, "不能重复计水"):
            self.db.record_usage("meter-01", {"account_id": source, "amount": 10, "meter_event_id": "M-1", "occurred_at": "2026-08-01"}, "meter")

    def test_third_party_and_self_approval_conflicts(self):
        source, target = self.accounts["北区水库"], self.accounts["河口灌区"]
        with self.assertRaisesRegex(DomainError, "最小留存"):
            self.db.create_transfer("alice", {"from_account_id": source, "to_account_id": target, "amount": 501, "effective_date": "2026-06-01"}, "editor")
        transfer = self.db.create_transfer("alice", {"from_account_id": source, "to_account_id": target, "amount": 100, "effective_date": "2026-06-01"}, "editor")
        with self.assertRaisesRegex(DomainError, "不能批准自己"):
            self.db.approve_transfer(transfer["id"], "alice", "reviewer")


def _make_pair(db: Database, quota_a: float = 1000.0, quota_b: float = 100.0):
    a = db.create_account("op", {"name": "甲方", "region": "r-a", "holder": "甲", "priority": 1,
                                 "valid_from": "2026-01-01", "valid_to": "2026-12-31", "quota": quota_a}, "editor")
    b = db.create_account("op", {"name": "乙方", "region": "r-b", "holder": "乙", "priority": 1,
                                 "valid_from": "2026-01-01", "valid_to": "2026-12-31", "quota": quota_b}, "editor")
    return a["id"], b["id"]


class DroughtOrderTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.db")
        self.accounts = seed_demo(self.db)

    def tearDown(self):
        self.tmp.cleanup()

    def test_order_recalculates_availability(self):
        source = self.accounts["北区水库"]
        order = self.db.upsert_drought_order(
            "dispatch", {"region": "upstream", "cap_fraction": 0.6,
                         "effective_from": "2026-06-01", "note": "伏旱"}, "dispatcher")
        self.assertEqual(order["cap_fraction"], 0.6)
        # quota=1000, used=100 -> limit=600, available=500
        info = self.db.available(source, as_of="2026-06-01")
        self.assertAlmostEqual(info["limit"], 600)
        self.assertAlmostEqual(info["available"], 500)
        # 生效日前限额令不适用
        before = self.db.available(source, as_of="2026-05-31")
        self.assertAlmostEqual(before["limit"], 1000)

    def test_only_dispatcher_may_issue_order(self):
        with self.assertRaisesRegex(DomainError, "旱情调度员"):
            self.db.upsert_drought_order("alice", {"region": "upstream", "cap_fraction": 0.6,
                                                   "effective_from": "2026-06-01"}, "editor")

    def test_order_rejects_over_limit_pending_but_keeps_recorded_usage(self):
        source = self.accounts["北区水库"]
        # 已登记取水 100（seed）保持不变；限额收紧到低于已用量
        order = self.db.upsert_drought_order(
            "dispatch", {"region": "upstream", "cap_fraction": 0.05,
                         "effective_from": "2026-06-01"}, "dispatcher")
        self.assertEqual(order["rejected_transfers"], [])
        info = self.db.available(source, as_of="2026-06-01")
        self.assertAlmostEqual(info["limit"], 50)
        self.assertAlmostEqual(info["available"], 0)
        # 已登记取水行原样保留，账户 used 不回改
        with self.db.connect() as conn:
            used = conn.execute("SELECT used FROM accounts WHERE id=?", (source,)).fetchone()["used"]
            records = conn.execute("SELECT COUNT(*) c FROM usage_records WHERE account_id=?",
                                   (source,)).fetchone()["c"]
        self.assertAlmostEqual(used, 100)
        self.assertEqual(records, 1)
        # 限额生效后新取水被拒
        with self.assertRaisesRegex(DomainError, "超过可用额度"):
            self.db.record_usage("m", {"account_id": source, "amount": 1,
                                       "meter_event_id": "X-1", "occurred_at": "2026-06-02"}, "meter")
        # 新转让同样被拒
        target = self.accounts["河口灌区"]
        with self.assertRaisesRegex(DomainError, "额度不足"):
            self.db.create_transfer("alice", {"from_account_id": source, "to_account_id": target,
                                              "amount": 1, "effective_date": "2026-06-02"}, "editor")

    def test_pending_transfers_auto_returned_fifo(self):
        a, b = _make_pair(self.db)
        t1 = self.db.create_transfer("alice", {"from_account_id": a, "to_account_id": b,
                                               "amount": 500, "effective_date": "2026-06-01"}, "editor")
        t2 = self.db.create_transfer("alice", {"from_account_id": a, "to_account_id": b,
                                               "amount": 300, "effective_date": "2026-06-01"}, "editor")
        order = self.db.upsert_drought_order(
            "dispatch", {"region": "r-a", "cap_fraction": 0.6,
                         "effective_from": "2026-06-01"}, "dispatcher")
        # limit=600: 先到的 500 保留，后到的 300 超限退回
        rejected_ids = [r["id"] for r in order["rejected_transfers"]]
        self.assertEqual(rejected_ids, [t2["id"]])
        transfers = {t["id"]: t for t in self.db.list_transfers()}
        self.assertEqual(transfers[t1["id"]]["status"], "pending")
        self.assertEqual(transfers[t2["id"]]["status"], "rejected")
        self.assertIn("旱期限额令", transfers[t2["id"]]["reason"])
        info = self.db.available(a, as_of="2026-06-01")
        self.assertAlmostEqual(info["reserved_outgoing"], 500)
        self.assertAlmostEqual(info["available"], 100)

    def test_surviving_pending_can_still_be_approved_under_order(self):
        a, b = _make_pair(self.db)
        t1 = self.db.create_transfer("alice", {"from_account_id": a, "to_account_id": b,
                                               "amount": 500, "effective_date": "2026-07-01"}, "editor")
        t2 = self.db.create_transfer("alice", {"from_account_id": a, "to_account_id": b,
                                               "amount": 200, "effective_date": "2026-07-01"}, "editor")
        self.db.upsert_drought_order("dispatch", {"region": "r-a", "cap_fraction": 0.6,
                                                  "effective_from": "2026-06-01"}, "dispatcher")
        # t2 被自动退回；t1 在限额内，批准时按当前限额复核仍可通过
        approved = self.db.approve_transfer(t1["id"], "bob", "reviewer")
        self.assertEqual(approved["status"], "approved")
        with self.db.connect() as conn:
            quota_a = conn.execute("SELECT quota FROM accounts WHERE id=?", (a,)).fetchone()["quota"]
            quota_b = conn.execute("SELECT quota FROM accounts WHERE id=?", (b,)).fetchone()["quota"]
        self.assertAlmostEqual(quota_a, 500)
        self.assertAlmostEqual(quota_b, 600)
        # 已退回的转让不能再批准
        with self.assertRaisesRegex(DomainError, "已处理"):
            self.db.approve_transfer(t2["id"], "bob", "reviewer")


class ConcurrentOccupationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.db")

    def tearDown(self):
        self.tmp.cleanup()

    def _run_concurrently(self, fn):
        outcomes: list[str] = []
        lock = threading.Lock()

        def worker(suffix: str):
            try:
                fn(suffix)
                result = "ok"
            except DomainError:
                result = "rejected"
            except Exception as exc:  # pragma: no cover - 测试失败时暴露真实错误
                result = f"error:{exc!r}"
            with lock:
                outcomes.append(result)

        threads = [threading.Thread(target=worker, args=(s,)) for s in ("one", "two")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        return outcomes

    def test_concurrent_usage_only_one_wins(self):
        a, _ = _make_pair(self.db, quota_a=100)
        outcomes = self._run_concurrently(
            lambda s: self.db.record_usage("m", {"account_id": a, "amount": 100,
                                                 "meter_event_id": f"EVT-{s}",
                                                 "occurred_at": "2026-06-01"}, "meter"))
        self.assertEqual(sorted(outcomes), ["ok", "rejected"])
        with self.db.connect() as conn:
            used = conn.execute("SELECT used FROM accounts WHERE id=?", (a,)).fetchone()["used"]
            posted = conn.execute("SELECT COUNT(*) c FROM usage_records WHERE account_id=?",
                                  (a,)).fetchone()["c"]
        self.assertAlmostEqual(used, 100)
        self.assertEqual(posted, 1)

    def test_concurrent_transfers_only_one_wins(self):
        a, b = _make_pair(self.db, quota_a=100)
        outcomes = self._run_concurrently(
            lambda s: self.db.create_transfer("alice", {"from_account_id": a, "to_account_id": b,
                                                        "amount": 100,
                                                        "effective_date": "2026-06-01"}, "editor"))
        self.assertEqual(sorted(outcomes), ["ok", "rejected"])
        pending = [t for t in self.db.list_transfers() if t["status"] == "pending"]
        self.assertEqual(len(pending), 1)
        self.assertAlmostEqual(pending[0]["amount"], 100)


class CrashRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.db")

    def tearDown(self):
        self.tmp.cleanup()

    def _seed_unposted_inbox(self, account_id: int, event_id: str, amount: float):
        """模拟服务在事件落盘后、入账前崩溃。"""
        with self.db.connect() as conn:
            conn.execute(
                "INSERT INTO meter_inbox(account_id,meter_event_id,amount,occurred_at,actor,created_at) VALUES(?,?,?,?,?,?)",
                (account_id, event_id, amount, "2026-06-01", "meter", "2026-06-01T00:00:00+00:00"))

    def test_recovery_posts_once_and_never_double_deducts(self):
        a, _ = _make_pair(self.db)
        self._seed_unposted_inbox(a, "RECOVER-1", 120)
        summary = self.db.recover_pending()
        self.assertEqual(summary["posted"], 1)
        with self.db.connect() as conn:
            used = conn.execute("SELECT used FROM accounts WHERE id=?", (a,)).fetchone()["used"]
            inbox = conn.execute("SELECT * FROM meter_inbox WHERE meter_event_id='RECOVER-1'").fetchone()
        self.assertAlmostEqual(used, 120)
        self.assertIsNotNone(inbox["posted_at"])
        # 再次恢复必须是无操作的：不重复入账、不重复扣减
        again = self.db.recover_pending()
        self.assertEqual(again["posted"], 0)
        with self.db.connect() as conn:
            used2 = conn.execute("SELECT used FROM accounts WHERE id=?", (a,)).fetchone()["used"]
            records = conn.execute("SELECT COUNT(*) c FROM usage_records WHERE account_id=?",
                                   (a,)).fetchone()["c"]
        self.assertAlmostEqual(used2, 120)
        self.assertEqual(records, 1)

    def test_recovery_after_usage_row_but_before_marker(self):
        """usage 行与 used 已提交而 inbox 标记未提交的窗口，恢复也不能二次扣减。"""
        a, _ = _make_pair(self.db)
        with self.db.connect() as conn:
            conn.execute(
                "INSERT INTO usage_records(account_id,meter_event_id,amount,occurred_at,actor,created_at) VALUES(?,?,?,?,?,?)",
                (a, "SPLIT-1", 70, "2026-06-01", "meter", "2026-06-01T00:00:00+00:00"))
            conn.execute("UPDATE accounts SET used=used+70 WHERE id=?", (a,))
            conn.execute(
                "INSERT INTO meter_inbox(account_id,meter_event_id,amount,occurred_at,actor,created_at) VALUES(?,?,?,?,?,?)",
                (a, "SPLIT-1", 70, "2026-06-01", "meter", "2026-06-01T00:00:00+00:00"))
        self.db.recover_pending()
        with self.db.connect() as conn:
            used = conn.execute("SELECT used FROM accounts WHERE id=?", (a,)).fetchone()["used"]
            records = conn.execute("SELECT COUNT(*) c FROM usage_records WHERE account_id=?",
                                   (a,)).fetchone()["c"]
        self.assertAlmostEqual(used, 70)
        self.assertEqual(records, 1)

    def test_idempotency_key_replays_cached_result(self):
        a, b = _make_pair(self.db, quota_a=100)
        payload = {"from_account_id": a, "to_account_id": b, "amount": 100,
                   "effective_date": "2026-06-01"}
        first = self.db.create_transfer("alice", payload, "editor", request_key="key-1")
        second = self.db.create_transfer("alice", payload, "editor", request_key="key-1")
        self.assertEqual(first["id"], second["id"])
        self.assertEqual(len([t for t in self.db.list_transfers() if t["status"] == "pending"]), 1)
        with self.assertRaisesRegex(DomainError, "已用于其他请求"):
            self.db.create_transfer("alice", {**payload, "amount": 99}, "editor", request_key="key-1")

    def test_rejected_meter_event_can_be_retried_with_new_payload(self):
        a, _ = _make_pair(self.db, quota_a=100)
        with self.assertRaises(DomainError):
            self.db.record_usage("m", {"account_id": a, "amount": 120,
                                       "meter_event_id": "EVT-R",
                                       "occurred_at": "2026-06-01"}, "meter")
        # 同一事件号按修正后的水量重试：成功且只入账一次
        result = self.db.record_usage("m", {"account_id": a, "amount": 80,
                                            "meter_event_id": "EVT-R",
                                            "occurred_at": "2026-06-01"}, "meter")
        self.assertAlmostEqual(result["amount"], 80)
        with self.db.connect() as conn:
            used = conn.execute("SELECT used FROM accounts WHERE id=?", (a,)).fetchone()["used"]
            records = conn.execute("SELECT COUNT(*) c FROM usage_records WHERE meter_event_id='EVT-R'").fetchone()["c"]
            inbox = conn.execute("SELECT COUNT(*) c FROM meter_inbox WHERE meter_event_id='EVT-R'").fetchone()["c"]
        self.assertAlmostEqual(used, 80)
        self.assertEqual(records, 1)
        self.assertEqual(inbox, 1)
        # 已入账后再提交同一事件仍是重复计水
        with self.assertRaisesRegex(DomainError, "不能重复计水"):
            self.db.record_usage("m", {"account_id": a, "amount": 1,
                                       "meter_event_id": "EVT-R",
                                       "occurred_at": "2026-06-01"}, "meter")


class ReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.db")
        self.a, self.b = _make_pair(self.db)

    def tearDown(self):
        self.tmp.cleanup()

    def _approve(self, amount: float) -> int:
        t = self.db.create_transfer("alice", {"from_account_id": self.a, "to_account_id": self.b,
                                              "amount": amount,
                                              "effective_date": "2026-06-01"}, "editor")
        self.db.approve_transfer(t["id"], "bob", "reviewer")
        return t["id"]

    def test_late_meter_mismatch_keeps_both_sides(self):
        transfer_id = self._approve(400)
        # 计量晚到，实际到水只有 380（输送损耗）
        result = self.db.record_usage("meter", {"account_id": self.b, "amount": 380,
                                                "meter_event_id": "LATE-1",
                                                "occurred_at": "2026-06-05",
                                                "transfer_id": transfer_id}, "meter")
        self.assertEqual(result["reconciliation"]["status"], "short")
        self.assertAlmostEqual(result["reconciliation"]["difference"], -20)
        # 两边记录都保留：批准的转让不改动，计量与取水费照常入账
        transfer = next(t for t in self.db.list_transfers() if t["id"] == transfer_id)
        self.assertEqual(transfer["status"], "approved")
        self.assertAlmostEqual(transfer["amount"], 400)
        report = self.db.reconciliation_report()
        entry = next(e for e in report["entries"] if e["transfer_id"] == transfer_id)
        self.assertAlmostEqual(entry["expected_amount"], 400)
        self.assertAlmostEqual(entry["metered_amount"], 380)
        self.assertAlmostEqual(entry["difference"], -20)
        self.assertEqual(entry["status"], "short")
        mismatches = [e for e in report["mismatches"] if e["transfer_id"] == transfer_id]
        self.assertEqual(len(mismatches), 1)

    def test_multiple_meter_events_aggregate_to_matched(self):
        transfer_id = self._approve(400)
        self.db.record_usage("meter", {"account_id": self.b, "amount": 380,
                                       "meter_event_id": "LATE-1", "occurred_at": "2026-06-05",
                                       "transfer_id": transfer_id}, "meter")
        second = self.db.record_usage("meter", {"account_id": self.b, "amount": 20,
                                                "meter_event_id": "LATE-2", "occurred_at": "2026-06-06",
                                                "transfer_id": transfer_id}, "meter")
        self.assertEqual(second["reconciliation"]["status"], "matched")
        self.assertAlmostEqual(second["reconciliation"]["metered_amount"], 400)

    def test_meter_arrives_before_approval_then_matches(self):
        t = self.db.create_transfer("alice", {"from_account_id": self.a, "to_account_id": self.b,
                                              "amount": 100, "effective_date": "2026-06-01"}, "editor")
        # 计量早于批准：先入账，不做对账
        result = self.db.record_usage("meter", {"account_id": self.b, "amount": 100,
                                                "meter_event_id": "EARLY-1", "occurred_at": "2026-06-02",
                                                "transfer_id": t["id"]}, "meter")
        self.assertNotIn("reconciliation", result)
        # 批准时补做对账
        approved = self.db.approve_transfer(t["id"], "bob", "reviewer")
        self.assertEqual(approved["reconciliation"]["status"], "matched")
        report = self.db.reconciliation_report()
        self.assertFalse([e for e in report["mismatches"] if e["transfer_id"] == t["id"]])

    def test_over_delivery_mismatch_reported(self):
        transfer_id = self._approve(400)
        result = self.db.record_usage("meter", {"account_id": self.b, "amount": 450,
                                                "meter_event_id": "OVER-1",
                                                "occurred_at": "2026-06-05",
                                                "transfer_id": transfer_id}, "meter")
        self.assertEqual(result["reconciliation"]["status"], "over")
        self.assertAlmostEqual(result["reconciliation"]["difference"], 50)


if __name__ == "__main__":
    unittest.main()
