import threading
import tempfile
import unittest
from datetime import date
from pathlib import Path

from app import Database, DomainError, seed_demo


class WaterRightsFlowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.db")
        self.accounts = seed_demo(self.db)
        self.source = self.accounts["北区水库"]
        self.target = self.accounts["河口灌区"]

    def tearDown(self):
        self.tmp.cleanup()

    def test_transfer_approval_usage_and_drought(self):
        transfer = self.db.create_transfer("alice", {"from_account_id": self.source, "to_account_id": self.target, "amount": 400, "effective_date": "2026-06-01"}, "editor")
        self.assertEqual(self.db.available(self.source)["reserved_outgoing"], 400)
        approved = self.db.approve_transfer(transfer["id"], "bob", "reviewer")
        self.assertEqual(approved["status"], "approved")
        usage = self.db.record_usage("meter-01", {"account_id": self.source, "amount": 150, "meter_event_id": "UP-JUL-1", "occurred_at": "2026-07-10"}, "meter")
        self.assertEqual(usage["amount"], 150)
        simulation = self.db.simulate_drought(1000, 0.3)
        self.assertAlmostEqual(sum(x["allocation"] for x in simulation["allocations"]) + simulation["unallocated"], 700)

    def test_duplicate_meter_event_and_pending_reservation(self):
        self.db.create_transfer("alice", {"from_account_id": self.source, "to_account_id": self.target, "amount": 500, "effective_date": "2026-06-01"}, "editor")
        with self.assertRaises(DomainError):
            self.db.create_transfer("alice", {"from_account_id": self.source, "to_account_id": self.target, "amount": 1, "effective_date": "2026-06-01"}, "editor")
        self.db.record_usage("meter-01", {"account_id": self.source, "amount": 10, "meter_event_id": "M-1", "occurred_at": "2026-08-01"}, "meter")
        with self.assertRaisesRegex(DomainError, "不能重复计水"):
            self.db.record_usage("meter-01", {"account_id": self.source, "amount": 10, "meter_event_id": "M-1", "occurred_at": "2026-08-01"}, "meter")

    def test_third_party_and_self_approval_conflicts(self):
        with self.assertRaisesRegex(DomainError, "最小留存"):
            self.db.create_transfer("alice", {"from_account_id": self.source, "to_account_id": self.target, "amount": 501, "effective_date": "2026-06-01"}, "editor")
        transfer = self.db.create_transfer("alice", {"from_account_id": self.source, "to_account_id": self.target, "amount": 100, "effective_date": "2026-06-01"}, "editor")
        with self.assertRaisesRegex(DomainError, "不能批准自己"):
            self.db.approve_transfer(transfer["id"], "alice", "reviewer")


class DroughtOrderTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.db")
        self.accounts = seed_demo(self.db)
        self.source = self.accounts["北区水库"]
        self.target = self.accounts["河口灌区"]

    def tearDown(self):
        self.tmp.cleanup()

    def issue(self, limit=600, scope="region", region="upstream", account_id=None,
              start="2026-06-01", actor="dispatch-1"):
        payload = {"scope": scope, "limit_amount": limit, "effective_date": start, "note": "伏旱压采"}
        if scope == "region":
            payload["region"] = region
        else:
            payload["account_id"] = account_id
        return self.db.issue_drought_order(actor, payload, "dispatcher")

    def test_dispatcher_cannot_rewrite_account_quota_directly(self):
        # 调度员唯一的手段是限额令；账户建改仍归 editor。
        with self.assertRaisesRegex(DomainError, "只有配额管理员"):
            self.db.create_account("dispatch-1", {"name": "X", "region": "upstream", "holder": "h",
                                                   "priority": 3, "quota": 1, "valid_from": "2026-01-01",
                                                   "valid_to": "2026-12-31"}, "dispatcher")

    def test_order_recomputes_availability(self):
        info_before = self.db.available(self.source, as_of="2026-06-01")
        self.assertIsNone(info_before["drought_cap"])
        self.assertEqual(info_before["available"], 900)
        self.issue(limit=500)
        info = self.db.available(self.source, as_of="2026-06-01")
        self.assertEqual(info["drought_cap"], 500)
        self.assertEqual(info["effective_quota"], 500)
        self.assertEqual(info["available"], 400)  # 500 - 已取 100
        # 生效日前不受影响
        before = self.db.available(self.source, as_of="2026-05-31")
        self.assertIsNone(before["drought_cap"])
        self.assertEqual(before["available"], 900)
        # 其他地区不受影响
        other = self.db.available(self.target, as_of="2026-06-01")
        self.assertIsNone(other["drought_cap"])

    def test_overlimit_pending_transfers_are_returned_fifo_and_usage_untouched(self):
        t1 = self.db.create_transfer("alice", {"from_account_id": self.source, "to_account_id": self.target,
                                                "amount": 300, "effective_date": "2026-06-01"}, "editor")
        t2 = self.db.create_transfer("alice", {"from_account_id": self.source, "to_account_id": self.target,
                                                "amount": 200, "effective_date": "2026-06-02"}, "editor")
        # 限额 550：已用 100，预占按 FIFO，300 保留（共 400），200 装不下 -> 退回
        summary = self.issue(limit=550)
        self.assertEqual(summary["effects"]["returned_transfers"], [t2["id"]])
        transfers = {t["id"]: t for t in self.db.list_transfers()}
        self.assertEqual(transfers[t1["id"]]["status"], "pending")
        self.assertEqual(transfers[t2["id"]]["status"], "returned")
        self.assertIn("旱期限额令", transfers[t2["id"]]["return_reason"])
        # 退回后预占释放，可用量按 550 重算
        info = self.db.available(self.source, as_of="2026-06-02")
        self.assertEqual(info["reserved_outgoing"], 300)
        self.assertEqual(info["available"], 150)
        # 已登记取水不改
        self.assertEqual(info["used"], 100)

    def test_approved_transfer_and_usage_survive_order(self):
        transfer = self.db.create_transfer("alice", {"from_account_id": self.source, "to_account_id": self.target,
                                                      "amount": 400, "effective_date": "2026-06-01"}, "editor")
        self.db.approve_transfer(transfer["id"], "bob", "reviewer")
        self.db.record_usage("m1", {"account_id": self.source, "amount": 50,
                                    "meter_event_id": "UP-X-1", "occurred_at": "2026-06-05"}, "meter")
        quota_after_approve = self.db.available(self.source)["quota"]
        self.issue(limit=200)
        info = self.db.available(self.source, as_of="2026-06-05")
        # 许可额度未被调度员改写；已批准转让不撤回，已登记取水不冲销
        self.assertEqual(info["quota"], quota_after_approve)
        self.assertEqual(info["used"], 150)
        self.assertEqual(info["available"], 50)  # min(600,200)-150
        self.assertEqual(self.db.list_transfers()[0]["status"], "approved")

    def test_new_transfer_and_usage_respect_order(self):
        # 使用没有上下游留存规则的新地区账户，单纯验证限额
        east1 = self.db.create_account("alice", {"name": "东郊水厂", "region": "east", "holder": "e1",
                                                  "priority": 1, "valid_from": "2026-01-01",
                                                  "valid_to": "2026-12-31", "quota": 300}, "editor")["id"]
        east2 = self.db.create_account("alice", {"name": "东郊用户", "region": "east", "holder": "e2",
                                                  "priority": 2, "valid_from": "2026-01-01",
                                                  "valid_to": "2026-12-31", "quota": 100}, "editor")["id"]
        self.issue(limit=200, region="east")
        # 可用 = 200
        self.db.create_transfer("alice", {"from_account_id": east1, "to_account_id": east2,
                                           "amount": 90, "effective_date": "2026-06-10"}, "editor")
        with self.assertRaisesRegex(DomainError, "可用额度不足"):
            self.db.create_transfer("alice", {"from_account_id": east1, "to_account_id": east2,
                                               "amount": 120, "effective_date": "2026-06-11"}, "editor")
        with self.assertRaisesRegex(DomainError, "取水超过可用额度"):
            self.db.record_usage("m2", {"account_id": east1, "amount": 120,
                                        "meter_event_id": "EAST-2", "occurred_at": "2026-06-12"}, "meter")
        # 预占释放后可以取
        pending = [t for t in self.db.list_transfers() if t["status"] == "pending"][0]
        self.db.reject_transfer(pending["id"], "bob", "reviewer")
        self.db.record_usage("m2", {"account_id": east1, "amount": 120,
                                    "meter_event_id": "EAST-2", "occurred_at": "2026-06-12"}, "meter")

    def test_order_update_relaxation_does_not_revive_returned(self):
        t1 = self.db.create_transfer("alice", {"from_account_id": self.source, "to_account_id": self.target,
                                                "amount": 450, "effective_date": "2026-06-01"}, "editor")
        order = self.issue(limit=400)
        self.assertEqual(self.db.list_transfers()[0]["status"], "returned")
        # 放宽到 900：退回记录保持退回（需重新发起），但可用量恢复
        self.db.update_drought_order(order["order"]["id"], "dispatch-1",
                                     {"limit_amount": 900}, "dispatcher")
        self.assertEqual(self.db.list_transfers()[0]["status"], "returned")
        info = self.db.available(self.source, as_of="2026-06-01")
        self.assertEqual(info["drought_cap"], 900)
        self.assertEqual(info["available"], 800)

    def test_reapply_order_is_idempotent(self):
        t1 = self.db.create_transfer("alice", {"from_account_id": self.source, "to_account_id": self.target,
                                                "amount": 450, "effective_date": "2026-06-01"}, "editor")
        order = self.issue(limit=400)
        # 再次执行（模拟重复投递/恢复）：不会重复产生 effect 或审计
        summary = self.db.apply_drought_order(order["order"]["id"])
        self.assertEqual(summary["returned_transfers"], [])
        effects = list(self.db.list_transfers())
        self.assertEqual(sum(1 for e in effects if e["status"] == "returned"), 1)

    def test_account_scoped_order(self):
        order = self.db.issue_drought_order("dispatch-1",
                                             {"scope": "account", "account_id": self.target,
                                              "limit_amount": 100, "effective_date": "2026-06-01"},
                                             "dispatcher")
        self.assertEqual(order["order"]["region"], "downstream")
        self.assertEqual(self.db.available(self.target, as_of="2026-06-01")["available"], 100)
        self.assertIsNone(self.db.available(self.source, as_of="2026-06-01")["drought_cap"])


class ConcurrencyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.db")
        self.accounts = seed_demo(self.db)
        self.source = self.accounts["北区水库"]
        self.target = self.accounts["河口灌区"]

    def tearDown(self):
        self.tmp.cleanup()

    def test_concurrent_usage_same_water_only_one_wins(self):
        # 可用 900：两笔各 800 的取水并发提交，只能成功一笔
        results: list[str] = []
        errors: list[str] = []
        barrier = threading.Barrier(2)

        def take(event_id: str):
            barrier.wait()
            try:
                self.db.record_usage("m", {"account_id": self.source, "amount": 800,
                                           "meter_event_id": event_id, "occurred_at": "2026-06-01"}, "meter")
                results.append(event_id)
            except DomainError as exc:
                errors.append(str(exc))

        t1 = threading.Thread(target=take, args=("C-1",))
        t2 = threading.Thread(target=take, args=("C-2",))
        t1.start(); t2.start(); t1.join(); t2.join()
        self.assertEqual(len(results), 1, results)
        self.assertEqual(len(errors), 1)
        self.assertEqual(self.db.available(self.source, as_of="2026-06-01")["used"], 900)

    def test_concurrent_transfers_never_double_reserve(self):
        # 可用 900：并发小额转让，成功预占总额不得超过可用量
        # （upstream 还有 0.4 最小留存约束，单笔上限对应留存 >=400）
        n = 20
        barrier = threading.Barrier(n)
        successes: list[int] = []
        lock = threading.Lock()

        def send(i: int):
            barrier.wait()
            try:
                t = self.db.create_transfer("alice", {"from_account_id": self.source,
                                                       "to_account_id": self.target, "amount": 120,
                                                       "effective_date": "2026-06-01"}, "editor")
                with lock:
                    successes.append(t["id"])
            except DomainError:
                pass

        threads = [threading.Thread(target=send, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        # 每成功一笔预留 120；留存约束最多允许预占 500，且绝不能超过可用 900
        self.assertLessEqual(len(successes), 7)
        self.assertGreaterEqual(len(successes), 1)
        reserved = self.db.available(self.source, as_of="2026-06-01")["reserved_outgoing"]
        self.assertLessEqual(reserved, 900 + 1e-6)

    def test_concurrent_usage_and_transfer_share_one_pool(self):
        barrier = threading.Barrier(2)
        out = {}

        def transfer():
            barrier.wait()
            try:
                out["transfer"] = self.db.create_transfer(
                    "alice", {"from_account_id": self.source, "to_account_id": self.target,
                              "amount": 900, "effective_date": "2026-06-01"}, "editor")
            except DomainError as exc:
                out["transfer_error"] = str(exc)

        def usage():
            barrier.wait()
            try:
                out["usage"] = self.db.record_usage(
                    "m", {"account_id": self.source, "amount": 900,
                          "meter_event_id": "C-MIX", "occurred_at": "2026-06-01"}, "meter")
            except DomainError as exc:
                out["usage_error"] = str(exc)

        t1 = threading.Thread(target=transfer)
        t2 = threading.Thread(target=usage)
        t1.start(); t2.start(); t1.join(); t2.join()
        exactly_one = ("transfer" in out) != ("usage" in out)
        self.assertTrue(exactly_one, out)
        info = self.db.available(self.source, as_of="2026-06-01")
        # 已用 100（种子）+ 新占用 900 = 1000，不会超过许可额度
        self.assertAlmostEqual(info["used"] + info["reserved_outgoing"], 1000, places=6)


class RecoveryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.db")
        self.accounts = seed_demo(self.db)
        self.source = self.accounts["北区水库"]
        self.target = self.accounts["河口灌区"]

    def issue(self, limit=600, start="2026-06-01", actor="dispatch-1"):
        return self.db.issue_drought_order(
            actor, {"scope": "region", "region": "upstream", "limit_amount": limit,
                    "effective_date": start, "note": "伏旱压采"}, "dispatcher")

    def tearDown(self):
        self.tmp.cleanup()

    def test_recover_unfinished_order_without_double_return(self):
        t1 = self.db.create_transfer("alice", {"from_account_id": self.source, "to_account_id": self.target,
                                                "amount": 450, "effective_date": "2026-06-01"}, "editor")
        order = self.issue(limit=300)
        # 模拟崩溃在 effects 执行中途：把 order 与 effect 都重置为未完成
        with self.db.connect() as conn:
            conn.execute("UPDATE drought_order_effects SET status='pending',applied_at=NULL WHERE order_id=?",
                         (order["order"]["id"],))
            conn.execute("UPDATE drought_orders SET apply_status='applying',applied_at=NULL WHERE id=?",
                         (order["order"]["id"],))
            # 同时把转让恢复为 pending（模拟退回动作未提交/未执行）
            conn.execute("UPDATE transfers SET status='pending',returned_at=NULL,return_reason=NULL WHERE id=?",
                         (t1["id"],))
        summary = self.db.recover()
        self.assertIn(order["order"]["id"], summary["orders_resumed"])
        self.assertEqual(summary["transfers_returned"], [t1["id"]])
        # 再恢复一次：幂等，没有新的退回/扣减
        again = self.db.recover()
        self.assertEqual(again["transfers_returned"], [])
        self.assertEqual(self.db.get_drought_order(order["order"]["id"])["apply_status"], "applied")
        returned = [t for t in self.db.list_transfers() if t["status"] == "returned"]
        self.assertEqual(len(returned), 1)

    def test_recover_staged_meter_event_without_double_deduction(self):
        # 模拟崩溃发生在事件已入库、尚未入账之后
        with self.db.connect() as conn:
            conn.execute(
                """INSERT INTO meter_events(meter_event_id,account_id,transfer_id,amount,occurred_at,
                      submitted_by,status,received_at)
                   VALUES('CRASH-1',?,NULL,80,'2026-06-03','meter-01','received',?)""",
                (self.source, "2026-06-03T00:00:00+00:00"),
            )
            event_id = conn.execute("SELECT id FROM meter_events WHERE meter_event_id='CRASH-1'").fetchone()[0]
        used_before = self.db.available(self.source)["used"]
        summary = self.db.recover()
        self.assertEqual(summary["meter_events_applied"], [event_id])
        self.assertEqual(self.db.available(self.source, as_of="2026-06-03")["used"], used_before + 80)
        # 二次恢复：状态已是 applied，不重复扣减
        again = self.db.recover()
        self.assertEqual(again["meter_events_applied"], [])
        self.assertEqual(self.db.available(self.source, as_of="2026-06-03")["used"], used_before + 80)
        events = self.db.list_meter_events()
        self.assertEqual(events[0]["status"], "applied")


class ReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Database(Path(self.tmp.name) / "test.db")
        self.accounts = seed_demo(self.db)
        self.source = self.accounts["北区水库"]
        self.target = self.accounts["河口灌区"]

    def tearDown(self):
        self.tmp.cleanup()

    def test_late_meter_mismatching_approved_transfer_keeps_both_sides(self):
        transfer = self.db.create_transfer("alice", {"from_account_id": self.source, "to_account_id": self.target,
                                                      "amount": 300, "effective_date": "2026-06-01"}, "editor")
        self.db.approve_transfer(transfer["id"], "bob", "reviewer")
        # 晚到计量实到 260，与批准的 300 对不上：取水照常入账（双边记录都在）
        usage = self.db.record_usage("meter-late", {"account_id": self.source, "amount": 260,
                                                     "meter_event_id": "LATE-1", "occurred_at": "2026-06-20",
                                                     "transfer_id": transfer["id"]}, "meter")
        self.assertEqual(usage["meter_event_id"], "LATE-1")
        entries = self.db.list_reconciliation()
        self.assertEqual(len(entries), 1)
        entry = entries[0]
        self.assertEqual(entry["transfer_id"], transfer["id"])
        self.assertEqual(entry["meter_event_id"], "LATE-1")
        self.assertEqual(entry["expected_amount"], 300)
        self.assertEqual(entry["actual_amount"], 260)
        self.assertEqual(entry["difference"], -40)
        self.assertIn("不符", entry["note"])
        # 事件台账、取水记录、转让记录三方都保留
        event = self.db.list_meter_events()[0]
        self.assertEqual(event["status"], "applied")
        self.assertEqual(event["usage_record_id"], usage["id"])
        self.assertEqual(self.db.list_transfers()[0]["status"], "approved")
        self.assertEqual(self.db.available(self.source, as_of="2026-06-20")["used"], 360)

    def test_late_meter_wrong_party_records_discrepancy(self):
        outsider = self.db.create_account("alice", {"name": "无关水站", "region": "east", "holder": "x",
                                                     "priority": 3, "valid_from": "2026-01-01",
                                                     "valid_to": "2026-12-31", "quota": 800}, "editor")["id"]
        transfer = self.db.create_transfer("alice", {"from_account_id": self.source, "to_account_id": self.target,
                                                      "amount": 100, "effective_date": "2026-06-01"}, "editor")
        self.db.approve_transfer(transfer["id"], "bob", "reviewer")
        self.db.record_usage("meter-late", {"account_id": outsider, "amount": 100,
                                            "meter_event_id": "LATE-2", "occurred_at": "2026-06-21",
                                            "transfer_id": transfer["id"]}, "meter")
        entries = self.db.list_reconciliation()
        self.assertEqual(len(entries), 1)
        self.assertIn("当事方", entries[0]["note"])

    def test_meter_linked_to_unapproved_transfer_rejected(self):
        transfer = self.db.create_transfer("alice", {"from_account_id": self.source, "to_account_id": self.target,
                                                      "amount": 100, "effective_date": "2026-06-01"}, "editor")
        with self.assertRaisesRegex(DomainError, "未批准"):
            self.db.record_usage("meter-late", {"account_id": self.source, "amount": 100,
                                                "meter_event_id": "LATE-3", "occurred_at": "2026-06-21",
                                                "transfer_id": transfer["id"]}, "meter")


if __name__ == "__main__":
    unittest.main()
