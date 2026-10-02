"""Cross-region water-right allocation and transfer service (standard library only)."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
from datetime import date, datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent
DEFAULT_DB = ROOT / "water_rights.db"
EPS = 1e-9


def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def parse_date(value: str, field: str = "日期") -> date:
    try:
        return date.fromisoformat(value)
    except (TypeError, ValueError) as exc:
        raise DomainError(f"{field}必须是 YYYY-MM-DD") from exc


class DomainError(Exception):
    def __init__(self, message: str, status: int = 400):
        super().__init__(message)
        self.status = status


class Database:
    def __init__(self, path: str | os.PathLike[str] = DEFAULT_DB):
        self.path = str(path)
        self._init_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    region TEXT NOT NULL,
                    holder TEXT NOT NULL,
                    priority INTEGER NOT NULL CHECK(priority BETWEEN 1 AND 5),
                    valid_from TEXT NOT NULL,
                    valid_to TEXT NOT NULL,
                    quota REAL NOT NULL CHECK(quota >= 0),
                    used REAL NOT NULL DEFAULT 0 CHECK(used >= 0),
                    created_at TEXT NOT NULL,
                    CHECK(valid_from <= valid_to)
                );
                CREATE TABLE IF NOT EXISTS transfers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    from_account_id INTEGER NOT NULL REFERENCES accounts(id),
                    to_account_id INTEGER NOT NULL REFERENCES accounts(id),
                    amount REAL NOT NULL CHECK(amount > 0),
                    effective_date TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_by TEXT NOT NULL,
                    approved_by TEXT,
                    created_at TEXT NOT NULL,
                    approved_at TEXT
                );
                CREATE TABLE IF NOT EXISTS usage_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER NOT NULL REFERENCES accounts(id),
                    meter_event_id TEXT NOT NULL,
                    amount REAL NOT NULL CHECK(amount > 0),
                    occurred_at TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(account_id, meter_event_id)
                );
                CREATE TABLE IF NOT EXISTS season_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    region TEXT NOT NULL,
                    month INTEGER NOT NULL CHECK(month BETWEEN 1 AND 12),
                    max_fraction REAL NOT NULL CHECK(max_fraction > 0 AND max_fraction <= 1),
                    note TEXT NOT NULL DEFAULT '',
                    UNIQUE(region, month)
                );
                CREATE TABLE IF NOT EXISTS impact_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_region TEXT NOT NULL,
                    target_region TEXT NOT NULL,
                    min_source_fraction REAL NOT NULL CHECK(min_source_fraction >= 0 AND min_source_fraction <= 1),
                    note TEXT NOT NULL DEFAULT '',
                    UNIQUE(source_region, target_region)
                );
                CREATE TABLE IF NOT EXISTS drought_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    region TEXT NOT NULL,
                    effective_from TEXT NOT NULL,
                    cap_fraction REAL NOT NULL CHECK(cap_fraction > 0 AND cap_fraction <= 1),
                    note TEXT NOT NULL DEFAULT '',
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(region, effective_from)
                );
                CREATE TABLE IF NOT EXISTS meter_inbox (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id INTEGER NOT NULL REFERENCES accounts(id),
                    meter_event_id TEXT NOT NULL,
                    amount REAL NOT NULL CHECK(amount > 0),
                    occurred_at TEXT NOT NULL,
                    transfer_id INTEGER REFERENCES transfers(id),
                    actor TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    posted_at TEXT,
                    reconciled_at TEXT,
                    error TEXT NOT NULL DEFAULT '',
                    UNIQUE(account_id, meter_event_id)
                );
                CREATE TABLE IF NOT EXISTS transfer_reconciliation (
                    transfer_id INTEGER PRIMARY KEY REFERENCES transfers(id),
                    expected_amount REAL NOT NULL,
                    metered_amount REAL NOT NULL DEFAULT 0,
                    difference REAL NOT NULL DEFAULT 0,
                    event_count INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS operations (
                    request_key TEXT PRIMARY KEY,
                    op_type TEXT NOT NULL,
                    fingerprint TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    response_json TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                """
            )
            self._migrate(conn)

    def _migrate(self, conn: sqlite3.Connection) -> None:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(transfers)")}
        if "reason" not in cols:
            conn.execute("ALTER TABLE transfers ADD COLUMN reason TEXT NOT NULL DEFAULT ''")

    def _audit(self, conn: sqlite3.Connection, actor: str, action: str, entity_type: str,
               entity_id: int | None, details: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO audit_log(actor,action,entity_type,entity_id,details,created_at) VALUES(?,?,?,?,?,?)",
            (actor, action, entity_type, entity_id, json.dumps(details, ensure_ascii=False), utcnow()),
        )

    # ---------- idempotency ledger ----------

    @staticmethod
    def _fingerprint(op_type: str, payload: Any) -> str:
        return f"{op_type}:{json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)}"

    def _idem_lookup(self, conn: sqlite3.Connection, request_key: str | None,
                     op_type: str, fingerprint: str) -> dict[str, Any] | None:
        """Return the cached response for a key already handled in a committed tx."""
        if not request_key:
            return None
        row = conn.execute("SELECT * FROM operations WHERE request_key=?", (request_key,)).fetchone()
        if not row:
            return None
        if row["op_type"] != op_type or row["fingerprint"] != fingerprint:
            raise DomainError("幂等键已用于其他请求，请更换 Idempotency-Key", 409)
        return json.loads(row["response_json"])

    def _idem_store(self, conn: sqlite3.Connection, request_key: str | None, op_type: str,
                    fingerprint: str, actor: str, result: dict[str, Any]) -> None:
        if not request_key:
            return
        payload = json.dumps(result, ensure_ascii=False, default=str)
        try:
            conn.execute(
                "INSERT INTO operations(request_key,op_type,fingerprint,actor,response_json,created_at) VALUES(?,?,?,?,?,?)",
                (request_key, op_type, fingerprint, actor, payload, utcnow()),
            )
        except sqlite3.IntegrityError:
            # A concurrent identical request committed first; its result wins.
            row = conn.execute("SELECT fingerprint,op_type,response_json FROM operations WHERE request_key=?",
                               (request_key,)).fetchone()
            if not row or row["op_type"] != op_type or row["fingerprint"] != fingerprint:
                raise DomainError("幂等键已用于其他请求，请更换 Idempotency-Key", 409)

    # ---------- accounts ----------

    def create_account(self, actor: str, payload: dict[str, Any], role: str = "editor",
                       request_key: str | None = None) -> dict[str, Any]:
        if role != "editor":
            raise DomainError("只有配额管理员可以创建账户", 403)
        name = str(payload.get("name", "")).strip()
        region = str(payload.get("region", "")).strip()
        holder = str(payload.get("holder", "")).strip()
        if not name or not region or not holder:
            raise DomainError("账户名称、地区和持有人不能为空")
        try:
            priority = int(payload.get("priority"))
            quota = float(payload.get("quota"))
        except (TypeError, ValueError) as exc:
            raise DomainError("优先级和额度必须是数值") from exc
        if not 1 <= priority <= 5 or quota < 0:
            raise DomainError("优先级应在 1 到 5 之间，额度不能为负")
        valid_from = parse_date(str(payload.get("valid_from", "")), "生效日期")
        valid_to = parse_date(str(payload.get("valid_to", "")), "失效日期")
        if valid_from > valid_to:
            raise DomainError("生效日期不能晚于失效日期")
        fp_payload = {"name": name, "region": region, "holder": holder, "priority": priority,
                      "quota": quota, "valid_from": valid_from.isoformat(), "valid_to": valid_to.isoformat()}
        fingerprint = self._fingerprint("account.create", fp_payload)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cached = self._idem_lookup(conn, request_key, "account.create", fingerprint)
            if cached is not None:
                return cached
            try:
                cur = conn.execute(
                    "INSERT INTO accounts(name,region,holder,priority,valid_from,valid_to,quota,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (name, region, holder, priority, valid_from.isoformat(), valid_to.isoformat(), quota, utcnow()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("账户名称已存在", 409) from exc
            self._audit(conn, actor, "account.created", "account", cur.lastrowid, {"name": name, "quota": quota})
            result = dict(conn.execute("SELECT * FROM accounts WHERE id=?", (cur.lastrowid,)).fetchone())
            self._idem_store(conn, request_key, "account.create", fingerprint, actor, result)
            return result

    def set_season_rule(self, actor: str, region: str, month: int, max_fraction: float,
                        note: str = "", role: str = "editor") -> dict[str, Any]:
        if role != "editor":
            raise DomainError("只有配额管理员可以设置季节规则", 403)
        if not 1 <= int(month) <= 12 or not 0 < float(max_fraction) <= 1:
            raise DomainError("月份或季节比例不合法")
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO season_rules(region,month,max_fraction,note) VALUES(?,?,?,?)
                   ON CONFLICT(region,month) DO UPDATE SET max_fraction=excluded.max_fraction,note=excluded.note""",
                (region.strip(), int(month), float(max_fraction), note),
            )
            self._audit(conn, actor, "season_rule.saved", "region", None, {"region": region, "month": month, "max_fraction": max_fraction})
        return {"region": region, "month": month, "max_fraction": max_fraction, "note": note}

    def set_impact_rule(self, actor: str, source_region: str, target_region: str, min_source_fraction: float,
                        note: str = "", role: str = "editor") -> dict[str, Any]:
        if role != "editor":
            raise DomainError("只有配额管理员可以设置第三方影响规则", 403)
        if not 0 <= float(min_source_fraction) <= 1:
            raise DomainError("最小留存比例必须在 0 到 1 之间")
        with self.connect() as conn:
            conn.execute(
                """INSERT INTO impact_rules(source_region,target_region,min_source_fraction,note) VALUES(?,?,?,?)
                   ON CONFLICT(source_region,target_region) DO UPDATE SET min_source_fraction=excluded.min_source_fraction,note=excluded.note""",
                (source_region, target_region, float(min_source_fraction), note),
            )
            self._audit(conn, actor, "impact_rule.saved", "region", None, {"source": source_region, "target": target_region, "min_fraction": min_source_fraction})
        return {"source_region": source_region, "target_region": target_region, "min_source_fraction": min_source_fraction, "note": note}

    # ---------- drought limit orders ----------

    def _active_order(self, conn: sqlite3.Connection, region: str, as_of: date) -> sqlite3.Row | None:
        return conn.execute(
            "SELECT * FROM drought_orders WHERE region=? AND effective_from<=? ORDER BY effective_from DESC, id DESC LIMIT 1",
            (region, as_of.isoformat()),
        ).fetchone()

    def _effective_limit(self, conn: sqlite3.Connection, account: sqlite3.Row,
                         as_of: date) -> tuple[float, sqlite3.Row | None]:
        """许可额度与当前生效限额令的较小值；已登记取水不回改。"""
        order = self._active_order(conn, account["region"], as_of)
        limit = float(account["quota"])
        if order:
            limit = min(limit, float(account["quota"]) * float(order["cap_fraction"]))
        return limit, order

    def _account_row(self, conn: sqlite3.Connection, account_id: int) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
        if not row:
            raise DomainError("水权账户不存在", 404)
        return row

    def _reserved_outgoing(self, conn: sqlite3.Connection, account_id: int,
                           exclude_transfer_id: int | None = None) -> float:
        if exclude_transfer_id is None:
            row = conn.execute(
                "SELECT COALESCE(SUM(amount),0) total FROM transfers WHERE from_account_id=? AND status='pending'",
                (account_id,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT COALESCE(SUM(amount),0) total FROM transfers WHERE from_account_id=? AND status='pending' AND id<>?",
                (account_id, exclude_transfer_id),
            ).fetchone()
        return float(row["total"])

    def _availability(self, conn: sqlite3.Connection, account: sqlite3.Row, as_of: date,
                      exclude_transfer_id: int | None = None) -> dict[str, Any]:
        limit, order = self._effective_limit(conn, account, as_of)
        reserved = self._reserved_outgoing(conn, int(account["id"]), exclude_transfer_id)
        available = max(0.0, limit - float(account["used"]) - reserved)
        return {"limit": limit, "reserved": reserved, "available": available, "order": order}

    def upsert_drought_order(self, actor: str, payload: dict[str, Any], role: str = "dispatcher",
                             request_key: str | None = None) -> dict[str, Any]:
        if role != "dispatcher":
            raise DomainError("只有旱情调度员可以发布限额令", 403)
        region = str(payload.get("region", "")).strip()
        if not region:
            raise DomainError("限额令必须指定地区")
        try:
            cap_fraction = float(payload.get("cap_fraction"))
        except (TypeError, ValueError) as exc:
            raise DomainError("限额比例必须是数值") from exc
        if not 0 < cap_fraction <= 1:
            raise DomainError("限额比例应在 0（不含）到 1 之间")
        effective_from = parse_date(str(payload.get("effective_from", "")), "生效日期")
        note = str(payload.get("note", ""))
        fp_payload = {"region": region, "cap_fraction": cap_fraction,
                      "effective_from": effective_from.isoformat(), "note": note}
        fingerprint = self._fingerprint("drought_order.upsert", fp_payload)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cached = self._idem_lookup(conn, request_key, "drought_order.upsert", fingerprint)
            if cached is not None:
                return cached
            now = utcnow()
            row = conn.execute(
                "SELECT id FROM drought_orders WHERE region=? AND effective_from=?",
                (region, effective_from.isoformat()),
            ).fetchone()
            if row:
                order_id = int(row["id"])
                conn.execute(
                    "UPDATE drought_orders SET cap_fraction=?,note=?,created_by=?,updated_at=? WHERE id=?",
                    (cap_fraction, note, actor, now, order_id),
                )
                action = "drought_order.updated"
            else:
                cur = conn.execute(
                    "INSERT INTO drought_orders(region,effective_from,cap_fraction,note,created_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                    (region, effective_from.isoformat(), cap_fraction, note, actor, now, now),
                )
                order_id = int(cur.lastrowid)
                action = "drought_order.issued"
            rejected = self._enforce_drought_order(conn, actor, order_id)
            order = dict(conn.execute("SELECT * FROM drought_orders WHERE id=?", (order_id,)).fetchone())
            order["rejected_transfers"] = rejected
            self._audit(conn, actor, action, "drought_order", order_id,
                        {"region": region, "cap_fraction": cap_fraction,
                         "effective_from": effective_from.isoformat(), "rejected_transfers": rejected})
            self._idem_store(conn, request_key, "drought_order.upsert", fingerprint, actor, order)
            return order

    def _enforce_drought_order(self, conn: sqlite3.Connection, actor: str, order_id: int) -> list[dict[str, Any]]:
        """重算受影响账户的可用量；超限的待审批转让按先到先得原则退回，已登记取水不动。"""
        order = conn.execute("SELECT * FROM drought_orders WHERE id=?", (order_id,)).fetchone()
        as_of = date.fromisoformat(order["effective_from"])
        rejected: list[dict[str, Any]] = []
        accounts = conn.execute("SELECT * FROM accounts WHERE region=? ORDER BY id", (order["region"],)).fetchall()
        for account in accounts:
            limit, _ = self._effective_limit(conn, account, as_of)
            room = limit - float(account["used"])
            kept = 0.0
            kept_transfers: list[int] = []
            account_rejected: list[int] = []
            pending = conn.execute(
                "SELECT t.*, a.region AS target_region FROM transfers t JOIN accounts a ON a.id=t.to_account_id "
                "WHERE t.from_account_id=? AND t.status='pending' ORDER BY t.id",
                (account["id"],),
            ).fetchall()
            for transfer in pending:
                amount = float(transfer["amount"])
                impact = conn.execute(
                    "SELECT * FROM impact_rules WHERE source_region=? AND target_region=?",
                    (account["region"], transfer["target_region"]),
                ).fetchone()
                minimum = float(account["quota"]) * float(impact["min_source_fraction"]) if impact else 0.0
                remaining = room - kept - amount
                if remaining >= -EPS and remaining >= minimum - EPS:
                    kept += amount
                    kept_transfers.append(int(transfer["id"]))
                    continue
                reason = f"旱期限额令 #{order_id} 生效（地区 {order['region']} 限额 {order['cap_fraction']:.2%}），额度不足"
                conn.execute(
                    "UPDATE transfers SET status='rejected',approved_by=?,approved_at=?,reason=? WHERE id=?",
                    (actor, utcnow(), reason, transfer["id"]),
                )
                item = {"id": int(transfer["id"]), "amount": amount, "reason": reason}
                rejected.append(item)
                account_rejected.append(int(transfer["id"]))
                self._audit(conn, actor, "transfer.auto_rejected", "transfer", int(transfer["id"]),
                            {"drought_order_id": order_id, "amount": amount, "limit": limit,
                             "used": float(account["used"]), "kept_pending": kept_transfers})
            self._audit(conn, actor, "account.availability_recomputed", "account", int(account["id"]),
                        {"drought_order_id": order_id, "quota": float(account["quota"]), "limit": limit,
                         "used": float(account["used"]), "reserved_kept": max(0.0, kept),
                         "available": max(0.0, room - kept), "rejected": account_rejected})
        return rejected

    def list_drought_orders(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM drought_orders ORDER BY effective_from DESC, id DESC").fetchall()
        return [dict(row) for row in rows]

    def available(self, account_id: int, as_of: str | None = None) -> dict[str, Any]:
        day = parse_date(as_of, "查询日期") if as_of else date.today()
        with self.connect() as conn:
            account = self._account_row(conn, account_id)
            info = self._availability(conn, account, day)
        return {"account_id": account_id, "available": info["available"],
                "reserved_outgoing": info["reserved"], "quota": account["quota"],
                "used": account["used"], "limit": info["limit"],
                "cap_fraction": info["order"]["cap_fraction"] if info["order"] else None,
                "order_effective_from": info["order"]["effective_from"] if info["order"] else None}

    # ---------- transfers ----------

    def create_transfer(self, actor: str, payload: dict[str, Any], role: str = "editor",
                        request_key: str | None = None) -> dict[str, Any]:
        if role != "editor":
            raise DomainError("只有水权编辑人员可以发起转让", 403)
        try:
            source_id = int(payload.get("from_account_id"))
            target_id = int(payload.get("to_account_id"))
            amount = float(payload.get("amount"))
        except (TypeError, ValueError) as exc:
            raise DomainError("账户和转让量必须是数值") from exc
        if source_id == target_id or amount <= 0:
            raise DomainError("转让账户不能相同，转让量必须大于 0")
        effective = parse_date(str(payload.get("effective_date", "")), "生效日期")
        fp_payload = {"from": source_id, "to": target_id, "amount": amount,
                      "effective_date": effective.isoformat()}
        fingerprint = self._fingerprint("transfer.create", fp_payload)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cached = self._idem_lookup(conn, request_key, "transfer.create", fingerprint)
            if cached is not None:
                return cached
            source = self._account_row(conn, source_id)
            target = self._account_row(conn, target_id)
            if not (source["valid_from"] <= effective.isoformat() <= source["valid_to"]):
                raise DomainError("转出账户在生效日无效", 409)
            if not (target["valid_from"] <= effective.isoformat() <= target["valid_to"]):
                raise DomainError("转入账户在生效日无效", 409)
            # 限额令按转让生效日生效；待审批转让立即预占，同一份水不能被二次占用。
            info = self._availability(conn, source, effective)
            if amount > info["available"] + EPS:
                if info["order"]:
                    raise DomainError("可用额度不足（已含旱期限额令与待审批预占），转让被拒绝", 409)
                raise DomainError("可用额度不足，待审批转让会预占额度", 409)
            # More critical users (smaller priority number) cannot transfer their
            # protected allocation to a less critical user.
            if int(source["priority"]) > int(target["priority"]):
                raise DomainError("不能把较低优先级水量转给更高优先级账户", 409)
            impact = conn.execute(
                "SELECT * FROM impact_rules WHERE source_region=? AND target_region=?",
                (source["region"], target["region"]),
            ).fetchone()
            if impact:
                remaining = info["available"] - amount
                minimum = float(source["quota"]) * float(impact["min_source_fraction"])
                if remaining + EPS < minimum:
                    raise DomainError("转让会违反下游第三方最小留存约束", 409)
            cur = conn.execute(
                "INSERT INTO transfers(from_account_id,to_account_id,amount,effective_date,created_by,created_at) VALUES(?,?,?,?,?,?)",
                (source_id, target_id, amount, effective.isoformat(), actor, utcnow()),
            )
            self._audit(conn, actor, "transfer.created", "transfer", cur.lastrowid,
                        {"source": source_id, "target": target_id, "amount": amount,
                         "effective_date": effective.isoformat(), "limit": info["limit"]})
            result = dict(conn.execute("SELECT * FROM transfers WHERE id=?", (cur.lastrowid,)).fetchone())
            self._idem_store(conn, request_key, "transfer.create", fingerprint, actor, result)
            return result

    def approve_transfer(self, transfer_id: int, actor: str, role: str = "reviewer",
                         request_key: str | None = None) -> dict[str, Any]:
        if role != "reviewer":
            raise DomainError("只有审核人可以批准转让", 403)
        fingerprint = self._fingerprint("transfer.approve", {"id": transfer_id})
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cached = self._idem_lookup(conn, request_key, "transfer.approve", fingerprint)
            if cached is not None:
                return cached
            transfer = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
            if not transfer:
                raise DomainError("转让记录不存在", 404)
            if transfer["status"] != "pending":
                raise DomainError("该转让已处理，不能重复批准", 409)
            if actor == transfer["created_by"]:
                raise DomainError("发起人不能批准自己的转让", 403)
            source = self._account_row(conn, transfer["from_account_id"])
            target = self._account_row(conn, transfer["to_account_id"])
            amount = float(transfer["amount"])
            effective = date.fromisoformat(transfer["effective_date"])
            # Compute against pending reservations other than this transfer,
            # including any drought cap in force on the effective date.
            info = self._availability(conn, source, effective, exclude_transfer_id=transfer_id)
            if amount > info["available"] + EPS:
                raise DomainError("审批时额度已被其他记录占用或限额令收紧，不能批准", 409)
            impact = conn.execute(
                "SELECT * FROM impact_rules WHERE source_region=? AND target_region=?",
                (source["region"], target["region"]),
            ).fetchone()
            if impact:
                minimum = float(source["quota"]) * float(impact["min_source_fraction"])
                if info["available"] - amount + EPS < minimum:
                    raise DomainError("审批时下游最小留存约束不再满足", 409)
            # The approved amount moves between quota balances. Keeping the
            # movement in the quota column preserves the original allocation
            # while making every downstream availability calculation consistent.
            conn.execute("UPDATE accounts SET quota=quota-? WHERE id=?", (amount, source["id"]))
            conn.execute("UPDATE accounts SET quota=quota+? WHERE id=?", (amount, target["id"]))
            conn.execute(
                "UPDATE transfers SET status='approved',approved_by=?,approved_at=?,reason=? WHERE id=?",
                (actor, utcnow(), transfer["reason"] or "审核批准", transfer_id),
            )
            # 计量事件可能早于批准到达（同样属于晚到对账），批准时补做对账。
            recon = self._aggregate_reconciliation(conn, transfer_id)
            self._audit(conn, actor, "transfer.approved", "transfer", transfer_id,
                        {"amount": amount, "reconciliation": recon})
            row = dict(conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone())
            if recon:
                row["reconciliation"] = recon
            self._idem_store(conn, request_key, "transfer.approve", fingerprint, actor, row)
            return row

    def reject_transfer(self, transfer_id: int, actor: str, role: str = "reviewer",
                        reason: str = "", request_key: str | None = None) -> dict[str, Any]:
        if role != "reviewer":
            raise DomainError("只有审核人可以退回转让", 403)
        fingerprint = self._fingerprint("transfer.reject", {"id": transfer_id, "reason": reason})
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            cached = self._idem_lookup(conn, request_key, "transfer.reject", fingerprint)
            if cached is not None:
                return cached
            row = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
            if not row or row["status"] != "pending":
                raise DomainError("转让不存在或已经处理", 409)
            if actor == row["created_by"]:
                raise DomainError("发起人不能自行退回", 403)
            conn.execute(
                "UPDATE transfers SET status='rejected',approved_by=?,approved_at=?,reason=? WHERE id=?",
                (actor, utcnow(), reason or "审核退回", transfer_id),
            )
            result = {"id": transfer_id, "status": "rejected"}
            self._audit(conn, actor, "transfer.rejected", "transfer", transfer_id, {"reason": reason})
            self._idem_store(conn, request_key, "transfer.reject", fingerprint, actor, result)
        return result

    # ---------- meter ingestion (durable inbox, crash-safe) ----------

    def _accept_meter_event(self, actor: str, payload: dict[str, Any]) -> tuple[sqlite3.Row, str]:
        """Durably accept a meter event in its own transaction.

        Returns (inbox_row, state) with state in {"new","posted","retry"}.
        "retry" means the same event previously failed to post and the inbox
        row has been refreshed with the corrected payload. A crash after this
        commit but before posting leaves a row that recover_pending() finishes
        exactly once.
        """
        try:
            account_id = int(payload.get("account_id"))
            amount = float(payload.get("amount"))
        except (TypeError, ValueError) as exc:
            raise DomainError("账户和取水量必须是数值") from exc
        meter_event_id = str(payload.get("meter_event_id", "")).strip()
        occurred = parse_date(str(payload.get("occurred_at", "")), "计量日期")
        transfer_id = payload.get("transfer_id")
        transfer_id = int(transfer_id) if transfer_id not in (None, "", []) else None
        if amount <= 0 or not meter_event_id:
            raise DomainError("取水量必须大于 0，计量事件编号不能为空")
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            account = self._account_row(conn, account_id)
            transfer = None
            if transfer_id is not None:
                transfer = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
                if not transfer:
                    raise DomainError("关联的转让记录不存在", 404)
                if int(transfer["to_account_id"]) != account_id:
                    raise DomainError("转让计量事件必须登记在受让方（转入）账户上", 409)
            try:
                cur = conn.execute(
                    "INSERT INTO meter_inbox(account_id,meter_event_id,amount,occurred_at,transfer_id,actor,created_at) VALUES(?,?,?,?,?,?,?)",
                    (account_id, meter_event_id, amount, occurred.isoformat(), transfer_id, actor, utcnow()),
                )
            except sqlite3.IntegrityError:
                row = conn.execute("SELECT * FROM meter_inbox WHERE account_id=? AND meter_event_id=?",
                                   (account_id, meter_event_id)).fetchone()
                if row["posted_at"]:
                    return row, "posted"
                # 前一次校验失败（超额/季节等）的同一事件允许按新载荷重试。
                conn.execute(
                    "UPDATE meter_inbox SET amount=?,occurred_at=?,transfer_id=?,actor=?,error='' WHERE id=?",
                    (amount, occurred.isoformat(), transfer_id, actor, row["id"]),
                )
                self._audit(conn, actor, "meter.retried", "meter_inbox", row["id"],
                            {"account_id": account_id, "meter_event_id": meter_event_id,
                             "amount": amount, "transfer_id": transfer_id})
                row = conn.execute("SELECT * FROM meter_inbox WHERE id=?", (row["id"],)).fetchone()
                return row, "retry"
            self._audit(conn, actor, "meter.accepted", "meter_inbox", cur.lastrowid,
                        {"account_id": account_id, "meter_event_id": meter_event_id,
                         "amount": amount, "transfer_id": transfer_id})
            row = conn.execute("SELECT * FROM meter_inbox WHERE id=?", (cur.lastrowid,)).fetchone()
            return row, "new"

    def _post_meter(self, conn: sqlite3.Connection, entry: sqlite3.Row) -> sqlite3.Row | None:
        """Post one inbox entry to usage_records/accounts. Idempotent per stage."""
        account = self._account_row(conn, int(entry["account_id"]))
        occurred = date.fromisoformat(entry["occurred_at"])
        amount = float(entry["amount"])
        if not (account["valid_from"] <= occurred.isoformat() <= account["valid_to"]):
            raise DomainError("取水日期不在许可有效期内")
        limit, order = self._effective_limit(conn, account, occurred)
        reserved = self._reserved_outgoing(conn, int(account["id"]))
        if amount > limit - float(account["used"]) - reserved + EPS:
            raise DomainError("取水超过可用额度（含旱期限额令与待审批预占）")
        season = conn.execute(
            "SELECT max_fraction FROM season_rules WHERE region=? AND month=?",
            (account["region"], occurred.month),
        ).fetchone()
        month_total = conn.execute(
            "SELECT COALESCE(SUM(amount),0) total FROM usage_records WHERE account_id=? AND substr(occurred_at,1,7)=?",
            (account["id"], occurred.strftime("%Y-%m")),
        ).fetchone()["total"]
        if season:
            cap = limit * float(season["max_fraction"])
            if float(month_total) + amount > cap + EPS:
                raise DomainError("本次取水超过该月份的季节配额（含旱期限额令）")
        inserted = True
        try:
            conn.execute(
                "INSERT INTO usage_records(account_id,meter_event_id,amount,occurred_at,actor,created_at) VALUES(?,?,?,?,?,?)",
                (account["id"], entry["meter_event_id"], amount, occurred.isoformat(),
                 entry["actor"], utcnow()),
            )
        except sqlite3.IntegrityError:
            # Crash recovery: the usage row already committed earlier; never
            # increment accounts.used a second time.
            inserted = False
        if inserted:
            conn.execute("UPDATE accounts SET used=used+? WHERE id=?", (amount, account["id"]))
        conn.execute("UPDATE meter_inbox SET posted_at=?,error='' WHERE id=?", (utcnow(), entry["id"]))
        self._audit(conn, entry["actor"], "usage.recorded", "account", int(account["id"]),
                    {"amount": amount, "occurred_at": occurred.isoformat(),
                     "meter_event_id": entry["meter_event_id"], "recovered_post": not inserted,
                     "drought_capped": bool(order)})
        return conn.execute(
            "SELECT * FROM usage_records WHERE account_id=? AND meter_event_id=?",
            (account["id"], entry["meter_event_id"]),
        ).fetchone()

    def _aggregate_reconciliation(self, conn: sqlite3.Connection, transfer_id: int) -> dict[str, Any] | None:
        """Aggregate all meter events linked to an approved transfer.

        Both sides are always kept: the approved transfer row is never altered,
        meter events stay in meter_inbox/usage_records; only the discrepancy
        summary is upserted.
        """
        transfer = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
        if not transfer or transfer["status"] != "approved":
            return None
        total = conn.execute(
            "SELECT COALESCE(SUM(amount),0) metered, COUNT(*) n FROM meter_inbox WHERE transfer_id=?",
            (transfer_id,),
        ).fetchone()
        metered, count = float(total["metered"]), int(total["n"])
        if count == 0:
            return None
        expected = float(transfer["amount"])
        difference = metered - expected
        if abs(difference) <= EPS:
            status = "matched"
        elif difference < 0:
            status = "short"
        else:
            status = "over"
        conn.execute(
            """INSERT INTO transfer_reconciliation(transfer_id,expected_amount,metered_amount,difference,event_count,status,updated_at)
               VALUES(?,?,?,?,?,?,?)
               ON CONFLICT(transfer_id) DO UPDATE SET expected_amount=excluded.expected_amount,
                 metered_amount=excluded.metered_amount,difference=excluded.difference,
                 event_count=excluded.event_count,status=excluded.status,updated_at=excluded.updated_at""",
            (transfer_id, expected, metered, difference, count, status, utcnow()),
        )
        conn.execute("UPDATE meter_inbox SET reconciled_at=? WHERE transfer_id=? AND reconciled_at IS NULL",
                     (utcnow(), transfer_id))
        return {"transfer_id": transfer_id, "expected_amount": expected, "metered_amount": metered,
                "difference": difference, "event_count": count, "status": status}

    def _reconcile_entry(self, conn: sqlite3.Connection, entry: sqlite3.Row) -> dict[str, Any] | None:
        if entry["transfer_id"] is None or entry["reconciled_at"]:
            return None
        return self._aggregate_reconciliation(conn, int(entry["transfer_id"]))

    def record_usage(self, actor: str, payload: dict[str, Any], role: str = "meter",
                     request_key: str | None = None) -> dict[str, Any]:
        if role not in {"meter", "editor"}:
            raise DomainError("只有计量员可以登记取水", 403)
        entry, state = self._accept_meter_event(actor, payload)
        if state == "posted":
            raise DomainError("计量事件已登记，不能重复计水", 409)
        fingerprint = self._fingerprint("usage.record", {
            "account_id": int(entry["account_id"]), "meter_event_id": entry["meter_event_id"]})
        try:
            with self.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                entry = conn.execute("SELECT * FROM meter_inbox WHERE id=?", (entry["id"],)).fetchone()
                if entry["posted_at"]:
                    raise DomainError("计量事件已登记，不能重复计水", 409)
                cached = self._idem_lookup(conn, request_key, "usage.record", fingerprint)
                if cached is not None:
                    return cached
                usage_row = self._post_meter(conn, entry)
                recon = self._reconcile_entry(conn, entry)
                if recon and recon["status"] != "matched":
                    self._audit(conn, actor, "reconciliation.mismatch", "transfer",
                                recon["transfer_id"], recon)
                result = dict(usage_row)
                if recon:
                    result["reconciliation"] = recon
                # 幂等台账与业务写入同一事务：崩溃要么都生效要么都不生效，不会重复扣减。
                self._idem_store(conn, request_key, "usage.record", fingerprint, actor, result)
                return result
        except DomainError as exc:
            # Persist the processing error against the inbox row (both sides are
            # retained); still attempt reconciliation against approved transfers.
            with self.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                fresh = conn.execute("SELECT * FROM meter_inbox WHERE id=?", (entry["id"],)).fetchone()
                if fresh and not fresh["posted_at"]:
                    conn.execute("UPDATE meter_inbox SET error=? WHERE id=?", (str(exc), entry["id"]))
                recon = self._reconcile_entry(conn, fresh) if fresh else None
                if recon and recon["status"] != "matched":
                    self._audit(conn, actor, "reconciliation.mismatch", "transfer",
                                recon["transfer_id"], recon)
            raise

    def recover_pending(self) -> dict[str, int]:
        """Resume durable meter work left unfinished by a crash.

        Every step is keyed by its own committed marker (posted_at /
        reconciled_at), so running recovery repeatedly never double-posts a
        usage record or double-increments accounts.used.
        """
        summary = {"posted": 0, "failed": 0, "reconciled": 0, "waiting_transfer": 0}
        with self.connect() as conn:
            unposted = conn.execute("SELECT id FROM meter_inbox WHERE posted_at IS NULL ORDER BY id").fetchall()
        for row in unposted:
            entry_id = int(row["id"])
            try:
                with self.connect() as conn:
                    conn.execute("BEGIN IMMEDIATE")
                    entry = conn.execute("SELECT * FROM meter_inbox WHERE id=?", (entry_id,)).fetchone()
                    if entry and not entry["posted_at"]:
                        self._post_meter(conn, entry)
                        summary["posted"] += 1
            except DomainError as exc:
                with self.connect() as conn:
                    conn.execute("UPDATE meter_inbox SET error=? WHERE id=?", (str(exc), entry_id))
                    self._audit(conn, "recovery", "meter.post_failed", "meter_inbox", entry_id,
                                {"error": str(exc)})
                summary["failed"] += 1
        with self.connect() as conn:
            open_entries = conn.execute(
                "SELECT id FROM meter_inbox WHERE transfer_id IS NOT NULL AND reconciled_at IS NULL ORDER BY id"
            ).fetchall()
        for row in open_entries:
            entry_id = int(row["id"])
            with self.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                entry = conn.execute("SELECT * FROM meter_inbox WHERE id=?", (entry_id,)).fetchone()
                if not entry or entry["reconciled_at"]:
                    continue
                transfer = conn.execute("SELECT status FROM transfers WHERE id=?",
                                        (entry["transfer_id"],)).fetchone()
                if transfer and transfer["status"] == "approved":
                    recon = self._aggregate_reconciliation(conn, int(entry["transfer_id"]))
                    if recon:
                        summary["reconciled"] += 1
                        if recon["status"] != "matched":
                            self._audit(conn, "recovery", "reconciliation.mismatch", "transfer",
                                        recon["transfer_id"], recon)
                elif transfer and transfer["status"] == "pending":
                    conn.execute("UPDATE meter_inbox SET error=? WHERE id=? AND error=''",
                                 ("等待转让批准后对账", entry_id))
                    summary["waiting_transfer"] += 1
                else:
                    # 转让已退回或缺失：计量记录保留，不再等待，差异留人工处理。
                    conn.execute("UPDATE meter_inbox SET error=? WHERE id=?",
                                 ("关联转让已退回或不存在，计量记录保留待人工对账", entry_id))
                    summary["waiting_transfer"] += 1
        return summary

    def reconciliation_report(self) -> dict[str, Any]:
        with self.connect() as conn:
            entries = [dict(r) for r in conn.execute(
                """SELECT r.*, t.from_account_id, t.to_account_id, t.amount AS transfer_amount,
                          t.effective_date, t.status AS transfer_status
                   FROM transfer_reconciliation r JOIN transfers t ON t.id=r.transfer_id
                   ORDER BY r.updated_at DESC""").fetchall()]
            waiting = [dict(r) for r in conn.execute(
                """SELECT m.* FROM meter_inbox m JOIN transfers t ON t.id=m.transfer_id
                   WHERE m.reconciled_at IS NULL ORDER BY m.id""").fetchall()]
            approved_open = [dict(r) for r in conn.execute(
                """SELECT t.* FROM transfers t LEFT JOIN transfer_reconciliation r ON r.transfer_id=t.id
                   WHERE t.status='approved' AND r.transfer_id IS NULL ORDER BY t.id""").fetchall()]
            failed = [dict(r) for r in conn.execute(
                "SELECT * FROM meter_inbox WHERE posted_at IS NULL ORDER BY id").fetchall()]
        mismatches = [e for e in entries if e["status"] != "matched"]
        return {"entries": entries, "mismatches": mismatches,
                "meters_waiting_for_transfer": waiting,
                "approved_transfers_without_meter": approved_open,
                "unposted_meter_events": failed}

    # ---------- simulation / listings ----------

    def simulate_drought(self, total_supply: float, reduction: float = 0.0, role: str = "viewer") -> dict[str, Any]:
        try:
            total_supply, reduction = float(total_supply), float(reduction)
        except (TypeError, ValueError) as exc:
            raise DomainError("供水量和削减比例必须是数值") from exc
        if total_supply < 0 or not 0 <= reduction < 1:
            raise DomainError("供水量不能为负，削减比例应在 0 到 1 之间")
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM accounts ORDER BY priority,name").fetchall()
        supply = total_supply * (1 - reduction)
        allocation: dict[int, float] = {}
        deficit: dict[int, float] = {}
        remaining = supply
        for priority in range(1, 6):
            group = [r for r in rows if int(r["priority"]) == priority]
            if not group:
                continue
            # During shortage, more critical rights receive their remaining
            # allocation first; only then does water flow to lower priorities.
            requested = sum(max(0.0, float(r["quota"]) - float(r["used"])) for r in group)
            take = min(remaining, requested)
            if requested <= 0:
                continue
            for row in group:
                quota_left = max(0.0, float(row["quota"]) - float(row["used"]))
                share = take * quota_left / requested
                allocation[int(row["id"])] = share
                deficit[int(row["id"])] = quota_left - share
            remaining -= take
            if remaining <= 1e-9:
                for lower in rows:
                    if int(lower["priority"]) > priority:
                        left = max(0.0, float(lower["quota"]) - float(lower["used"]))
                        allocation[int(lower["id"])] = 0.0
                        deficit[int(lower["id"])] = left
                break
        return {"total_supply": total_supply, "reduction": reduction, "effective_supply": supply,
                "unallocated": remaining, "allocations": [
                    {"account_id": int(r["id"]), "name": r["name"], "priority": r["priority"],
                     "allocation": allocation.get(int(r["id"]), 0.0), "deficit": deficit.get(int(r["id"]), 0.0)}
                    for r in rows
                ]}

    def list_accounts(self) -> list[dict[str, Any]]:
        today = date.today()
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM accounts ORDER BY id").fetchall()
            result = []
            for row in rows:
                item = dict(row)
                info = self._availability(conn, row, today)
                item["available"] = info["available"]
                item["limit"] = info["limit"]
                item["cap_fraction"] = info["order"]["cap_fraction"] if info["order"] else None
                result.append(item)
        return result

    def list_transfers(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM transfers ORDER BY id DESC").fetchall()
        return [dict(row) for row in rows]

    def audit(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM audit_log ORDER BY id DESC").fetchall()
        return [dict(row) for row in rows]


def seed_demo(db: Database) -> dict[str, int]:
    if db.list_accounts():
        return {str(a["name"]): int(a["id"]) for a in db.list_accounts()}
    upstream = db.create_account("alice", {"name": "北区水库", "region": "upstream", "holder": "北区水务公司", "priority": 1, "valid_from": "2026-01-01", "valid_to": "2026-12-31", "quota": 1000}, "editor")
    downstream = db.create_account("alice", {"name": "河口灌区", "region": "downstream", "holder": "河口合作社", "priority": 2, "valid_from": "2026-01-01", "valid_to": "2026-12-31", "quota": 500}, "editor")
    db.set_season_rule("alice", "upstream", 7, 0.35, "夏季上限", "editor")
    db.set_impact_rule("alice", "upstream", "downstream", 0.4, "保障河口最小生态流量", "editor")
    db.record_usage("meter-01", {"account_id": upstream["id"], "amount": 100, "meter_event_id": "UP-2026-0001", "occurred_at": "2026-03-01"}, "meter")
    return {"北区水库": int(upstream["id"]), "河口灌区": int(downstream["id"])}


class Handler(BaseHTTPRequestHandler):
    db: Database
    server_version = "WaterRights/1.0"

    def _send(self, payload: Any, status: int = 200) -> None:
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _html(self) -> None:
        data = (ROOT / "static" / "index.html").read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError as exc:
            raise DomainError("请求体不是合法 JSON") from exc

    def _auth(self) -> tuple[str, str]:
        return self.headers.get("X-User", "anonymous"), self.headers.get("X-Role", "viewer")

    def _idem_key(self) -> str | None:
        return self.headers.get("Idempotency-Key") or self.headers.get("X-Idempotency-Key")

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path in {"/", "/index.html"}:
                return self._html()
            if parsed.path == "/api/health":
                return self._send({"ok": True})
            if parsed.path == "/api/accounts":
                return self._send({"accounts": self.db.list_accounts()})
            if parsed.path == "/api/transfers":
                return self._send({"transfers": self.db.list_transfers()})
            if parsed.path == "/api/audit":
                return self._send({"audit": self.db.audit()})
            if parsed.path == "/api/drought/orders":
                return self._send({"orders": self.db.list_drought_orders()})
            if parsed.path == "/api/reconciliation":
                return self._send(self.db.reconciliation_report())
            if parsed.path.startswith("/api/accounts/") and parsed.path.endswith("/available"):
                account_id = int(parsed.path.split("/")[3])
                q = parse_qs(parsed.query)
                return self._send(self.db.available(account_id, q.get("as_of", [None])[0]))
            if parsed.path == "/api/drought/simulate":
                q = parse_qs(parsed.query)
                return self._send(self.db.simulate_drought(float(q.get("supply", ["0"])[0]), float(q.get("reduction", ["0"])[0])))
            raise DomainError("接口不存在", 404)
        except (ValueError, DomainError) as exc:
            self._send({"error": str(exc)}, getattr(exc, "status", 400))
        except sqlite3.Error as exc:
            self._send({"error": f"数据库繁忙，请重试: {exc}"}, 503)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            actor, role = self._auth()
            key = self._idem_key()
            body = self._body()
            parts = [p for p in parsed.path.split("/") if p]
            if parts == ["api", "accounts"]:
                return self._send(self.db.create_account(actor, body, role, key), 201)
            if parts == ["api", "rules", "season"]:
                return self._send(self.db.set_season_rule(actor, str(body.get("region", "")), int(body.get("month", 0)), body.get("max_fraction"), str(body.get("note", "")), role), 201)
            if parts == ["api", "rules", "impact"]:
                return self._send(self.db.set_impact_rule(actor, str(body.get("source_region", "")), str(body.get("target_region", "")), body.get("min_source_fraction"), str(body.get("note", "")), role), 201)
            if parts == ["api", "drought", "orders"]:
                return self._send(self.db.upsert_drought_order(actor, body, role, key), 201)
            if parts == ["api", "transfers"]:
                return self._send(self.db.create_transfer(actor, body, role, key), 201)
            if len(parts) == 4 and parts[:2] == ["api", "transfers"] and parts[3] == "approve":
                return self._send(self.db.approve_transfer(int(parts[2]), actor, role,
                                                           request_key=key), 200)
            if len(parts) == 4 and parts[:2] == ["api", "transfers"] and parts[3] == "reject":
                return self._send(self.db.reject_transfer(int(parts[2]), actor, role,
                                                          str(body.get("reason", "")), key), 200)
            if parts == ["api", "usage"]:
                return self._send(self.db.record_usage(actor, body, role, key), 201)
            raise DomainError("接口不存在", 404)
        except (ValueError, TypeError, DomainError) as exc:
            self._send({"error": str(exc)}, getattr(exc, "status", 400))

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"[water] {self.address_string()} - {fmt % args}")


def main() -> None:
    parser = argparse.ArgumentParser(description="跨区域水资源使用权分配与转让服务")
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "8007")))
    parser.add_argument("--db", default=os.getenv("WATER_DB", str(DEFAULT_DB)))
    parser.add_argument("--init", action="store_true", help="创建数据库并写入示例账户")
    args = parser.parse_args()
    db = Database(args.db)
    if args.init:
        seed_demo(db)
        print(f"initialized database at {args.db}")
        return
    # 崩溃重启后先把未完成的计量入账/对账推进完，各阶段靠提交标记保证不重复扣减。
    recovered = db.recover_pending()
    print(f"recovery: {recovered}")
    Handler.db = db
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"water-rights listening on http://127.0.0.1:{args.port} (db={args.db})")
    server.serve_forever()


if __name__ == "__main__":
    main()
