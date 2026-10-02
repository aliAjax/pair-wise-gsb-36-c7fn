"""Cross-region water-right allocation and transfer service (standard library only)."""
from __future__ import annotations

import argparse
import calendar
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
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor TEXT NOT NULL,
                    action TEXT NOT NULL,
                    entity_type TEXT NOT NULL,
                    entity_id INTEGER,
                    details TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                /* 旱期限额令：调度员不再直接改 accounts.quota，而是下达限额令。
                   账户有效额度 = min(quota, 所有生效限额令的最小 limit_amount)。 */
                CREATE TABLE IF NOT EXISTS drought_orders (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope TEXT NOT NULL CHECK(scope IN ('region','account')),
                    region TEXT,
                    account_id INTEGER REFERENCES accounts(id),
                    limit_amount REAL NOT NULL CHECK(limit_amount >= 0),
                    effective_date TEXT NOT NULL,
                    end_date TEXT,
                    note TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','revoked')),
                    apply_status TEXT NOT NULL DEFAULT 'pending'
                        CHECK(apply_status IN ('pending','applying','applied')),
                    issued_by TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    applied_at TEXT,
                    CHECK((scope='region' AND region IS NOT NULL AND region <> '')
                          OR (scope='account' AND account_id IS NOT NULL)),
                    CHECK(end_date IS NULL OR effective_date <= end_date)
                );
                /* 限额令影响台账：每条要退回的转让占一行，崩溃恢复靠它去重。 */
                CREATE TABLE IF NOT EXISTS drought_order_effects (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id INTEGER NOT NULL REFERENCES drought_orders(id),
                    transfer_id INTEGER NOT NULL REFERENCES transfers(id),
                    action TEXT NOT NULL CHECK(action IN ('return_transfer')),
                    status TEXT NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','done')),
                    created_at TEXT NOT NULL,
                    applied_at TEXT,
                    UNIQUE(order_id, transfer_id)
                );
                /* 计量事件收件台账：先登记再处理，重启后据此恢复，不会重复扣减。 */
                CREATE TABLE IF NOT EXISTS meter_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    meter_event_id TEXT NOT NULL UNIQUE,
                    account_id INTEGER NOT NULL REFERENCES accounts(id),
                    transfer_id INTEGER REFERENCES transfers(id),
                    amount REAL NOT NULL CHECK(amount > 0),
                    occurred_at TEXT NOT NULL,
                    submitted_by TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'received'
                        CHECK(status IN ('received','applied','failed')),
                    usage_record_id INTEGER REFERENCES usage_records(id),
                    error TEXT,
                    received_at TEXT NOT NULL,
                    applied_at TEXT
                );
                /* 对账差异：晚到计量与已批准转让对不上时，两边记录都保留，差异落表。 */
                CREATE TABLE IF NOT EXISTS reconciliation_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    kind TEXT NOT NULL,
                    transfer_id INTEGER,
                    meter_event_id TEXT,
                    account_id INTEGER,
                    expected_amount REAL,
                    actual_amount REAL,
                    difference REAL NOT NULL,
                    note TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL,
                    resolved INTEGER NOT NULL DEFAULT 0
                );
                """
            )
            self._migrate(conn)

    @staticmethod
    def _migrate(conn: sqlite3.Connection) -> None:
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(transfers)")}
        for name, ddl in (
            ("returned_by", "ALTER TABLE transfers ADD COLUMN returned_by TEXT"),
            ("returned_at", "ALTER TABLE transfers ADD COLUMN returned_at TEXT"),
            ("return_reason", "ALTER TABLE transfers ADD COLUMN return_reason TEXT"),
        ):
            if name not in cols:
                conn.execute(ddl)

    def _audit(self, conn: sqlite3.Connection, actor: str, action: str, entity_type: str,
               entity_id: int | None, details: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO audit_log(actor,action,entity_type,entity_id,details,created_at) VALUES(?,?,?,?,?,?)",
            (actor, action, entity_type, entity_id, json.dumps(details, ensure_ascii=False), utcnow()),
        )

    def create_account(self, actor: str, payload: dict[str, Any], role: str = "editor") -> dict[str, Any]:
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
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            try:
                cur = conn.execute(
                    "INSERT INTO accounts(name,region,holder,priority,valid_from,valid_to,quota,created_at) VALUES(?,?,?,?,?,?,?,?)",
                    (name, region, holder, priority, valid_from.isoformat(), valid_to.isoformat(), quota, utcnow()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("账户名称已存在", 409) from exc
            self._audit(conn, actor, "account.created", "account", cur.lastrowid, {"name": name, "quota": quota})
            return dict(conn.execute("SELECT * FROM accounts WHERE id=?", (cur.lastrowid,)).fetchone())

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

    def _account_row(self, conn: sqlite3.Connection, account_id: int) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM accounts WHERE id=?", (account_id,)).fetchone()
        if not row:
            raise DomainError("水权账户不存在", 404)
        return row

    def _reserved_outgoing(self, conn: sqlite3.Connection, account_id: int) -> float:
        row = conn.execute("SELECT COALESCE(SUM(amount),0) total FROM transfers WHERE from_account_id=? AND status='pending'", (account_id,)).fetchone()
        return float(row["total"])

    @staticmethod
    def _order_matches(order: sqlite3.Row | dict[str, Any], region: str, account_id: int) -> bool:
        if order["scope"] == "region":
            return order["region"] == region
        return int(order["account_id"]) == int(account_id)

    @staticmethod
    def _order_active_on(order: sqlite3.Row | dict[str, Any], as_of_iso: str) -> bool:
        if order["status"] != "active":
            return False
        if order["effective_date"] > as_of_iso:
            return False
        return order["end_date"] is None or order["end_date"] >= as_of_iso

    def _active_order_limit(self, conn: sqlite3.Connection, region: str, account_id: int,
                            as_of_iso: str, include: sqlite3.Row | dict[str, Any] | None = None) -> float | None:
        """Smallest active, applied drought cap for the account on the given date.

        ``include`` lets the order currently being applied count even before it is
        marked applied, so the cascade plan sees the cap it is about to enforce.
        """
        row = conn.execute(
            """
            SELECT MIN(limit_amount) AS m FROM drought_orders
            WHERE status='active' AND apply_status='applied'
              AND effective_date<=? AND (end_date IS NULL OR end_date>=?)
              AND ((scope='region' AND region=?) OR (scope='account' AND account_id=?))
            """,
            (as_of_iso, as_of_iso, region, account_id),
        ).fetchone()
        limit = None if row["m"] is None else float(row["m"])
        if include is not None and self._order_active_on(include, as_of_iso) \
                and self._order_matches(include, region, account_id):
            value = float(include["limit_amount"])
            limit = value if limit is None else min(limit, value)
        return limit

    def _effective_quota(self, conn: sqlite3.Connection, account: sqlite3.Row, as_of_iso: str,
                         include: sqlite3.Row | dict[str, Any] | None = None) -> tuple[float, float | None]:
        cap = self._active_order_limit(conn, account["region"], account["id"], as_of_iso, include)
        quota = float(account["quota"])
        return (quota if cap is None else min(quota, cap)), cap

    def available(self, account_id: int, as_of: str | None = None) -> dict[str, Any]:
        as_of_iso = parse_date(as_of, "查询日期").isoformat() if as_of else date.today().isoformat()
        with self.connect() as conn:
            account = self._account_row(conn, account_id)
            reserved = self._reserved_outgoing(conn, account_id)
            effective, cap = self._effective_quota(conn, account, as_of_iso)
            value = max(0.0, effective - float(account["used"]) - reserved)
        return {"account_id": account_id, "as_of": as_of_iso, "quota": account["quota"],
                "drought_cap": cap, "effective_quota": effective, "used": account["used"],
                "reserved_outgoing": reserved, "available": value}

    # ------------------------------------------------------------------
    # 旱期限额令
    # ------------------------------------------------------------------
    def issue_drought_order(self, actor: str, payload: dict[str, Any], role: str = "dispatcher") -> dict[str, Any]:
        if role not in {"dispatcher", "editor"}:
            raise DomainError("只有旱情调度员可以下限额令", 403)
        scope = str(payload.get("scope", "")).strip()
        if scope not in {"region", "account"}:
            raise DomainError("限额令范围必须是 region 或 account")
        region = str(payload.get("region", "")).strip()
        account_id = payload.get("account_id")
        if scope == "region" and not region:
            raise DomainError("地区限额令必须填写地区")
        if scope == "account":
            if account_id in (None, ""):
                raise DomainError("账户限额令必须填写账户")
            try:
                account_id = int(account_id)
            except (TypeError, ValueError) as exc:
                raise DomainError("账户编号必须是整数") from exc
        else:
            account_id = None
        try:
            limit_amount = float(payload.get("limit_amount"))
        except (TypeError, ValueError) as exc:
            raise DomainError("限额必须是数值") from exc
        if limit_amount < 0:
            raise DomainError("限额不能为负")
        effective = parse_date(str(payload.get("effective_date") or date.today().isoformat()), "生效日期")
        end_value = str(payload.get("end_date") or "").strip()
        end = parse_date(end_value, "失效日期") if end_value else None
        if end and end < effective:
            raise DomainError("失效日期不能早于生效日期")
        note = str(payload.get("note", ""))
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            if scope == "account":
                target = self._account_row(conn, account_id)
                region = target["region"]
            cur = conn.execute(
                """INSERT INTO drought_orders(scope,region,account_id,limit_amount,effective_date,end_date,note,
                      status,apply_status,issued_by,version,created_at,updated_at)
                   VALUES(?,?,?,?,?,?,?, 'active','pending', ?,1,?,?)""",
                (scope, region, account_id, limit_amount, effective.isoformat(),
                 end.isoformat() if end else None, note, actor, utcnow(), utcnow()),
            )
            order_id = cur.lastrowid
            self._audit(conn, actor, "drought_order.issued", "drought_order", order_id,
                        {"scope": scope, "region": region, "account_id": account_id,
                         "limit_amount": limit_amount, "effective_date": effective.isoformat()})
        # 级联处理在独立的持久化步骤中完成，崩溃后可由 recover() 继续。
        summary = self.apply_drought_order(order_id, actor=actor)
        return {"order": self.get_drought_order(order_id), "effects": summary}

    def update_drought_order(self, order_id: int, actor: str, payload: dict[str, Any],
                             role: str = "dispatcher") -> dict[str, Any]:
        if role not in {"dispatcher", "editor"}:
            raise DomainError("只有旱情调度员可以调整限额令", 403)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            order = conn.execute("SELECT * FROM drought_orders WHERE id=?", (order_id,)).fetchone()
            if not order:
                raise DomainError("限额令不存在", 404)
            limit_amount = float(order["limit_amount"])
            if "limit_amount" in payload and payload["limit_amount"] is not None:
                try:
                    limit_amount = float(payload["limit_amount"])
                except (TypeError, ValueError) as exc:
                    raise DomainError("限额必须是数值") from exc
                if limit_amount < 0:
                    raise DomainError("限额不能为负")
            end = order["end_date"]
            if "end_date" in payload:
                end_value = str(payload.get("end_date") or "").strip()
                end = parse_date(end_value, "失效日期").isoformat() if end_value else None
            if end and end < order["effective_date"]:
                raise DomainError("失效日期不能早于生效日期")
            status = str(payload.get("status") or order["status"])
            if status not in {"active", "revoked"}:
                raise DomainError("限额令状态只能是 active 或 revoked")
            note = str(payload.get("note", order["note"]))
            # 重新触发级联：已退回的转让不会因限额放松而自动复活。
            conn.execute(
                """UPDATE drought_orders SET limit_amount=?, end_date=?, status=?, note=?,
                      apply_status='pending', applied_at=NULL, version=version+1, updated_at=?
                   WHERE id=?""",
                (limit_amount, end, status, note, utcnow(), order_id),
            )
            self._audit(conn, actor, "drought_order.updated", "drought_order", order_id,
                        {"limit_amount": limit_amount, "end_date": end, "status": status,
                         "version": int(order["version"]) + 1})
        summary = self.apply_drought_order(order_id, actor=actor) if status == "active" else {"returned_transfers": []}
        return {"order": self.get_drought_order(order_id), "effects": summary}

    def get_drought_order(self, order_id: int) -> dict[str, Any]:
        with self.connect() as conn:
            row = conn.execute("SELECT * FROM drought_orders WHERE id=?", (order_id,)).fetchone()
            if not row:
                raise DomainError("限额令不存在", 404)
            return dict(row)

    def list_drought_orders(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM drought_orders ORDER BY id DESC").fetchall()
        return [dict(row) for row in rows]

    def _affected_accounts(self, conn: sqlite3.Connection, order: sqlite3.Row) -> list[sqlite3.Row]:
        if order["scope"] == "region":
            return conn.execute("SELECT * FROM accounts WHERE region=? ORDER BY id", (order["region"],)).fetchall()
        return [self._account_row(conn, int(order["account_id"]))]

    def _plan_drought_order(self, order_id: int) -> None:
        """Decide which pending transfers the order must return (FIFO). Idempotent."""
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            order = conn.execute("SELECT * FROM drought_orders WHERE id=?", (order_id,)).fetchone()
            if not order:
                raise DomainError("限额令不存在", 404)
            if order["status"] != "active" or order["apply_status"] == "applied":
                return
            for account in self._affected_accounts(conn, order):
                effective, _cap = self._effective_quota(conn, account, order["effective_date"], include=order)
                headroom = effective - float(account["used"])
                reserved_kept = 0.0
                pending = conn.execute(
                    "SELECT * FROM transfers WHERE from_account_id=? AND status='pending' ORDER BY id",
                    (account["id"],),
                ).fetchall()
                for transfer in pending:
                    amount = float(transfer["amount"])
                    # 按发起先后保留预占；第一个装不下的及因此超限的转让退回。
                    if reserved_kept + amount <= headroom + EPS:
                        reserved_kept += amount
                        continue
                    conn.execute(
                        """INSERT OR IGNORE INTO drought_order_effects(order_id,transfer_id,action,status,created_at)
                           VALUES(?,?,'return_transfer','pending',?)""",
                        (order_id, transfer["id"], utcnow()),
                    )
            conn.execute("UPDATE drought_orders SET apply_status='applying',updated_at=? WHERE id=? AND apply_status<>'applied'",
                         (utcnow(), order_id))

    def _run_drought_effects(self, order_id: int) -> list[int]:
        """Execute pending effects one per transaction; conditional updates make
        re-runs (recovery or duplicate delivery) harmless — no double returns."""
        returned: list[int] = []
        while True:
            with self.connect() as conn:
                conn.execute("BEGIN IMMEDIATE")
                effect = conn.execute(
                    "SELECT * FROM drought_order_effects WHERE order_id=? AND status='pending' ORDER BY id LIMIT 1",
                    (order_id,),
                ).fetchone()
                if effect is None:
                    break
                transfer_id = int(effect["transfer_id"])
                transfer = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
                reason = f"旱期限额令 #{order_id} 收紧，可用量不足以覆盖待审批转让"
                if transfer and transfer["status"] == "pending":
                    conn.execute(
                        "UPDATE transfers SET status='returned',returned_by=?,returned_at=?,return_reason=? WHERE id=? AND status='pending'",
                        (f"drought-order-{order_id}", utcnow(), reason, transfer_id),
                    )
                    self._audit(conn, f"drought-order-{order_id}", "transfer.returned", "transfer", transfer_id,
                                {"order_id": order_id, "amount": transfer["amount"], "reason": reason})
                    returned.append(transfer_id)
                conn.execute("UPDATE drought_order_effects SET status='done',applied_at=? WHERE id=?",
                             (utcnow(), effect["id"]))
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            pending = conn.execute(
                "SELECT COUNT(*) c FROM drought_order_effects WHERE order_id=? AND status='pending'", (order_id,)
            ).fetchone()["c"]
            if pending == 0:
                conn.execute("UPDATE drought_orders SET apply_status='applied',applied_at=?,updated_at=? WHERE id=?",
                             (utcnow(), utcnow(), order_id))
        return returned

    def apply_drought_order(self, order_id: int, actor: str | None = None) -> dict[str, Any]:
        self._plan_drought_order(order_id)
        returned = self._run_drought_effects(order_id)
        if returned and actor:
            with self.connect() as conn:
                self._audit(conn, actor, "drought_order.applied", "drought_order", order_id,
                            {"returned_transfers": returned})
        return {"returned_transfers": returned}

    def create_transfer(self, actor: str, payload: dict[str, Any], role: str = "editor") -> dict[str, Any]:
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
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            source = self._account_row(conn, source_id)
            target = self._account_row(conn, target_id)
            if not (source["valid_from"] <= effective.isoformat() <= source["valid_to"]):
                raise DomainError("转出账户在生效日无效", 409)
            if not (target["valid_from"] <= effective.isoformat() <= target["valid_to"]):
                raise DomainError("转入账户在生效日无效", 409)
            reserved = self._reserved_outgoing(conn, source_id)
            source_quota, cap = self._effective_quota(conn, source, effective.isoformat())
            available = source_quota - float(source["used"]) - reserved
            if amount > available + EPS:
                hint = "（含旱期限额）" if cap is not None else ""
                raise DomainError(f"可用额度不足{hint}，待审批转让会预占额度", 409)
            # More critical users (smaller priority number) cannot transfer their
            # protected allocation to a less critical user.
            if int(source["priority"]) > int(target["priority"]):
                raise DomainError("不能把较低优先级水量转给更高优先级账户", 409)
            impact = conn.execute(
                "SELECT * FROM impact_rules WHERE source_region=? AND target_region=?",
                (source["region"], target["region"]),
            ).fetchone()
            if impact:
                minimum = float(source["quota"]) * float(impact["min_source_fraction"])
                if available - amount + EPS < minimum:
                    raise DomainError("转让会违反下游第三方最小留存约束", 409)
            cur = conn.execute(
                "INSERT INTO transfers(from_account_id,to_account_id,amount,effective_date,created_by,created_at) VALUES(?,?,?,?,?,?)",
                (source_id, target_id, amount, effective.isoformat(), actor, utcnow()),
            )
            self._audit(conn, actor, "transfer.created", "transfer", cur.lastrowid,
                        {"source": source_id, "target": target_id, "amount": amount, "effective_date": effective.isoformat()})
            return dict(conn.execute("SELECT * FROM transfers WHERE id=?", (cur.lastrowid,)).fetchone())

    def approve_transfer(self, transfer_id: int, actor: str, role: str = "reviewer") -> dict[str, Any]:
        if role != "reviewer":
            raise DomainError("只有审核人可以批准转让", 403)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
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
            # Compute against pending reservations other than this transfer.
            other_reserved = conn.execute(
                "SELECT COALESCE(SUM(amount),0) total FROM transfers WHERE from_account_id=? AND status='pending' AND id<>?",
                (source["id"], transfer_id),
            ).fetchone()["total"]
            source_quota, cap = self._effective_quota(conn, source, transfer["effective_date"])
            available = source_quota - float(source["used"]) - float(other_reserved)
            if amount > available + EPS:
                hint = "（含旱期限额）" if cap is not None else ""
                raise DomainError(f"审批时额度已被其他记录占用{hint}，不能批准", 409)
            impact = conn.execute(
                "SELECT * FROM impact_rules WHERE source_region=? AND target_region=?",
                (source["region"], target["region"]),
            ).fetchone()
            if impact:
                minimum = float(source["quota"]) * float(impact["min_source_fraction"])
                if available - amount + EPS < minimum:
                    raise DomainError("审批时下游最小留存约束不再满足", 409)
            # The approved amount moves between quota balances. Keeping the
            # movement in the quota column preserves the original allocation
            # while making every downstream availability calculation consistent.
            conn.execute("UPDATE accounts SET quota=quota-? WHERE id=?", (amount, source["id"]))
            conn.execute("UPDATE accounts SET quota=quota+? WHERE id=?", (amount, target["id"]))
            conn.execute("UPDATE transfers SET status='approved',approved_by=?,approved_at=? WHERE id=?", (actor, utcnow(), transfer_id))
            self._audit(conn, actor, "transfer.approved", "transfer", transfer_id, {"amount": amount})
            row = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
        return dict(row)

    def reject_transfer(self, transfer_id: int, actor: str, role: str = "reviewer") -> dict[str, Any]:
        if role != "reviewer":
            raise DomainError("只有审核人可以退回转让", 403)
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM transfers WHERE id=?", (transfer_id,)).fetchone()
            if not row or row["status"] != "pending":
                raise DomainError("转让不存在或已经处理", 409)
            if actor == row["created_by"]:
                raise DomainError("发起人不能自行退回", 403)
            conn.execute("UPDATE transfers SET status='rejected',approved_by=?,approved_at=? WHERE id=?", (actor, utcnow(), transfer_id))
            self._audit(conn, actor, "transfer.rejected", "transfer", transfer_id, {})
        return {"id": transfer_id, "status": "rejected"}

    # ------------------------------------------------------------------
    # 计量取水（事件先入台账，再入账，支持崩溃恢复与晚到对账）
    # ------------------------------------------------------------------
    def record_usage(self, actor: str, payload: dict[str, Any], role: str = "meter") -> dict[str, Any]:
        if role not in {"meter", "editor"}:
            raise DomainError("只有计量员可以登记取水", 403)
        try:
            account_id = int(payload.get("account_id"))
            amount = float(payload.get("amount"))
        except (TypeError, ValueError) as exc:
            raise DomainError("账户和取水量必须是数值") from exc
        meter_event_id = str(payload.get("meter_event_id", "")).strip()
        occurred = parse_date(str(payload.get("occurred_at", "")), "计量日期")
        transfer_id = payload.get("transfer_id")
        if transfer_id not in (None, ""):
            try:
                transfer_id = int(transfer_id)
            except (TypeError, ValueError) as exc:
                raise DomainError("转让编号必须是整数") from exc
        else:
            transfer_id = None
        if amount <= 0 or not meter_event_id:
            raise DomainError("取水量必须大于 0，计量事件编号不能为空")
        # 第一步：事件持久化进台账。即使接下来服务崩溃，重启后也能从这里继续。
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._account_row(conn, account_id)
            existing = conn.execute(
                "SELECT id,status FROM meter_events WHERE meter_event_id=?", (meter_event_id,)
            ).fetchone()
            if existing is not None:
                # 已入账 -> 幂等拒绝；此前因校验失败搁置 -> 允许用同一事件号重试
                if existing["status"] == "applied":
                    raise DomainError("计量事件已登记，不能重复计水", 409)
                event_id = int(existing["id"])
            else:
                try:
                    cur = conn.execute(
                        """INSERT INTO meter_events(meter_event_id,account_id,transfer_id,amount,occurred_at,
                              submitted_by,status,received_at)
                           VALUES(?,?,?,?,?,?,'received',?)""",
                        (meter_event_id, account_id, transfer_id, amount, occurred.isoformat(), actor, utcnow()),
                    )
                except sqlite3.IntegrityError as exc:
                    raise DomainError("计量事件已登记，不能重复计水", 409) from exc
                event_id = cur.lastrowid
        return self._apply_meter_event(event_id)

    def _apply_meter_event(self, event_id: int) -> dict[str, Any]:
        with self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            event = conn.execute("SELECT * FROM meter_events WHERE id=?", (event_id,)).fetchone()
            if not event:
                raise DomainError("计量事件不存在", 404)
            if event["status"] == "applied":
                # 恢复时重复执行：直接返回既有入账，绝不二次扣减。
                existing = conn.execute("SELECT * FROM usage_records WHERE id=?", (event["usage_record_id"],)).fetchone()
                return dict(existing) if existing else {"meter_event_id": event["meter_event_id"], "status": "applied"}
            account = self._account_row(conn, event["account_id"])
            amount = float(event["amount"])
            occurred_iso = event["occurred_at"]
            meter_event_id = event["meter_event_id"]
            actor = event["submitted_by"]

            linked_transfer = None
            mismatch: dict[str, Any] | None = None
            if event["transfer_id"] is not None:
                linked_transfer = conn.execute(
                    "SELECT * FROM transfers WHERE id=?", (event["transfer_id"],)
                ).fetchone()
                if not linked_transfer or linked_transfer["status"] != "approved":
                    conn.execute("UPDATE meter_events SET status='failed',error=? WHERE id=?",
                                 ("关联转让不存在或尚未批准", event_id))
                    raise DomainError("计量事件关联的转让不存在或未批准，无法入账", 409)
                expected = float(linked_transfer["amount"])
                party = {int(linked_transfer["from_account_id"]), int(linked_transfer["to_account_id"])}
                notes: list[str] = []
                if abs(amount - expected) > EPS:
                    notes.append(f"实到 {amount:g} 与批准转让 {expected:g} 不符")
                if int(account["id"]) not in party:
                    notes.append("计量账户不是转让当事方")
                if notes:
                    difference = amount - expected
                    mismatch = {"difference": difference, "expected": expected,
                                "note": "；".join(notes)}

            if not (account["valid_from"] <= occurred_iso <= account["valid_to"]):
                raise DomainError("取水日期不在许可有效期内", 409)

            # 与已批准转让对不上的晚到计量：以计量实物量为准照常入账，
            # 双边记录都保留，差异写入对账表，不再做额度/季节上限拦截。
            if mismatch is None:
                effective, _cap = self._effective_quota(conn, account, occurred_iso)
                reserved = self._reserved_outgoing(conn, account["id"])
                available = effective - float(account["used"]) - reserved
                if amount > available + EPS:
                    raise DomainError("取水超过可用额度（含旱期限额）", 409)
                season = conn.execute(
                    "SELECT max_fraction FROM season_rules WHERE region=? AND month=?",
                    (account["region"], int(occurred_iso[5:7])),
                ).fetchone()
                month_total = conn.execute(
                    "SELECT COALESCE(SUM(amount),0) total FROM usage_records WHERE account_id=? AND substr(occurred_at,1,7)=?",
                    (account["id"], occurred_iso[:7]),
                ).fetchone()["total"]
                if season:
                    cap_season = effective * float(season["max_fraction"])
                    if float(month_total) + amount > cap_season + EPS:
                        raise DomainError("本次取水超过该月份的季节配额", 409)

            try:
                cur = conn.execute(
                    "INSERT INTO usage_records(account_id,meter_event_id,amount,occurred_at,actor,created_at) VALUES(?,?,?,?,?,?)",
                    (account["id"], meter_event_id, amount, occurred_iso, actor, utcnow()),
                )
            except sqlite3.IntegrityError as exc:
                raise DomainError("计量事件已登记，不能重复计水", 409) from exc
            conn.execute("UPDATE accounts SET used=used+? WHERE id=?", (amount, account["id"]))
            usage_id = cur.lastrowid

            if mismatch is not None:
                recon_cur = conn.execute(
                    """INSERT INTO reconciliation_entries(kind,transfer_id,meter_event_id,account_id,
                          expected_amount,actual_amount,difference,note,created_at)
                       VALUES(?,?,?,?,?,?,?,?,?)""",
                    ("meter_transfer_mismatch", linked_transfer["id"], meter_event_id, account["id"],
                     mismatch["expected"], amount, mismatch["difference"], mismatch["note"], utcnow()),
                )
                self._audit(conn, actor, "reconciliation.diff", "transfer", linked_transfer["id"],
                            {"meter_event_id": meter_event_id, "difference": mismatch["difference"],
                             "note": mismatch["note"], "reconciliation_id": recon_cur.lastrowid})

            conn.execute("UPDATE meter_events SET status='applied',usage_record_id=?,applied_at=?,error=NULL WHERE id=?",
                         (usage_id, utcnow(), event_id))
            self._audit(conn, actor, "usage.recorded", "account", account["id"],
                        {"amount": amount, "occurred_at": occurred_iso, "meter_event_id": meter_event_id,
                         "reconciled": mismatch is not None})
            row = conn.execute("SELECT * FROM usage_records WHERE id=?", (usage_id,)).fetchone()
            return dict(row)

    def list_reconciliation(self, include_resolved: bool = True) -> list[dict[str, Any]]:
        sql = "SELECT * FROM reconciliation_entries"
        if not include_resolved:
            sql += " WHERE resolved=0"
        sql += " ORDER BY id DESC"
        with self.connect() as conn:
            return [dict(r) for r in conn.execute(sql).fetchall()]

    def list_meter_events(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            return [dict(r) for r in conn.execute("SELECT * FROM meter_events ORDER BY id DESC").fetchall()]

    def recover(self) -> dict[str, Any]:
        """Resume unfinished work after a crash: drought-order cascades and
        staged meter events. Every step is conditional/idempotent, so recovery
        never returns a transfer twice and never double-deducts usage."""
        resumed_orders: list[int] = []
        returned_all: list[int] = []
        with self.connect() as conn:
            orders = conn.execute(
                "SELECT id FROM drought_orders WHERE status='active' AND apply_status<>'applied' ORDER BY id"
            ).fetchall()
        for row in orders:
            order_id = int(row["id"])
            returned = self.apply_drought_order(order_id).get("returned_transfers", [])
            resumed_orders.append(order_id)
            returned_all.extend(returned)

        applied_events: list[int] = []
        failed_events: list[dict[str, Any]] = []
        with self.connect() as conn:
            events = conn.execute("SELECT id FROM meter_events WHERE status='received' ORDER BY id").fetchall()
        for row in events:
            event_id = int(row["id"])
            try:
                self._apply_meter_event(event_id)
                applied_events.append(event_id)
            except DomainError as exc:
                with self.connect() as conn:
                    conn.execute("UPDATE meter_events SET status='failed',error=? WHERE id=? AND status='received'",
                                 (str(exc), event_id))
                failed_events.append({"id": event_id, "error": str(exc)})
        return {"orders_resumed": resumed_orders, "transfers_returned": returned_all,
                "meter_events_applied": applied_events, "meter_events_failed": failed_events}

    def simulate_drought(self, total_supply: float, reduction: float = 0.0, role: str = "viewer") -> dict[str, Any]:
        try:
            total_supply, reduction = float(total_supply), float(reduction)
        except (TypeError, ValueError) as exc:
            raise DomainError("供水量和削减比例必须是数值") from exc
        if total_supply < 0 or not 0 <= reduction < 1:
            raise DomainError("供水量不能为负，削减比例应在 0 到 1 之间")
        today = date.today().isoformat()
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM accounts ORDER BY priority,name").fetchall()
            capped = [(r, self._effective_quota(conn, r, today)[0]) for r in rows]
        supply = total_supply * (1 - reduction)
        allocation: dict[int, float] = {}
        deficit: dict[int, float] = {}
        remaining = supply
        for priority in range(1, 6):
            group = [item for item in capped if int(item[0]["priority"]) == priority]
            if not group:
                continue
            # During shortage, more critical rights receive their remaining
            # allocation first; only then does water flow to lower priorities.
            requested = sum(max(0.0, eff - float(r["used"])) for r, eff in group)
            take = min(remaining, requested)
            if requested <= 0:
                continue
            for row, eff in group:
                quota_left = max(0.0, eff - float(row["used"]))
                share = take * quota_left / requested
                allocation[int(row["id"])] = share
                deficit[int(row["id"])] = quota_left - share
            remaining -= take
            if remaining <= EPS:
                for lower, eff in capped:
                    if int(lower["priority"]) > priority:
                        allocation[int(lower["id"])] = 0.0
                        deficit[int(lower["id"])] = max(0.0, eff - float(lower["used"]))
                break
        return {"total_supply": total_supply, "reduction": reduction, "effective_supply": supply,
                "unallocated": remaining, "allocations": [
                    {"account_id": int(r["id"]), "name": r["name"], "priority": r["priority"],
                     "allocation": allocation.get(int(r["id"]), 0.0), "deficit": deficit.get(int(r["id"]), 0.0)}
                    for r, _eff in capped]}

    def list_accounts(self) -> list[dict[str, Any]]:
        today = date.today().isoformat()
        with self.connect() as conn:
            rows = conn.execute("SELECT * FROM accounts ORDER BY id").fetchall()
            result = []
            for row in rows:
                item = dict(row)
                effective, cap = self._effective_quota(conn, row, today)
                reserved = self._reserved_outgoing(conn, int(row["id"]))
                item["drought_cap"] = cap
                item["effective_quota"] = effective
                item["available"] = max(0.0, effective - float(row["used"]) - reserved)
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
        data = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
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
                return self._send({"entries": self.db.list_reconciliation()})
            if parsed.path == "/api/meter-events":
                return self._send({"events": self.db.list_meter_events()})
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

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            actor, role = self._auth()
            body = self._body()
            parts = [p for p in parsed.path.split("/") if p]
            if parts == ["api", "accounts"]:
                return self._send(self.db.create_account(actor, body, role), 201)
            if parts == ["api", "rules", "season"]:
                return self._send(self.db.set_season_rule(actor, str(body.get("region", "")), int(body.get("month", 0)), body.get("max_fraction"), str(body.get("note", "")), role), 201)
            if parts == ["api", "rules", "impact"]:
                return self._send(self.db.set_impact_rule(actor, str(body.get("source_region", "")), str(body.get("target_region", "")), body.get("min_source_fraction"), str(body.get("note", "")), role), 201)
            if parts == ["api", "drought", "orders"]:
                return self._send(self.db.issue_drought_order(actor, body, role), 201)
            if len(parts) == 4 and parts[:3] == ["api", "drought", "orders"]:
                return self._send(self.db.update_drought_order(int(parts[3]), actor, body, role))
            if parts == ["api", "drought", "recover"]:
                if role not in {"dispatcher", "editor"}:
                    raise DomainError("只有旱情调度员可以触发恢复", 403)
                return self._send(self.db.recover())
            if parts == ["api", "transfers"]:
                return self._send(self.db.create_transfer(actor, body, role), 201)
            if len(parts) == 4 and parts[:2] == ["api", "transfers"] and parts[3] == "approve":
                return self._send(self.db.approve_transfer(int(parts[2]), actor, role))
            if len(parts) == 4 and parts[:2] == ["api", "transfers"] and parts[3] == "reject":
                return self._send(self.db.reject_transfer(int(parts[2]), actor, role))
            if parts == ["api", "usage"]:
                return self._send(self.db.record_usage(actor, body, role), 201)
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
    # 崩溃重启后，先把未完成的限额令级联和计量事件补齐，再对外服务。
    summary = db.recover()
    if any(summary.values()):
        print(f"[water] recovery summary: {summary}", flush=True)
    Handler.db = db
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"water-rights listening on http://127.0.0.1:{args.port} (db={args.db})", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
