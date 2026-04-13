#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import shutil
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

DB_PATH = Path("data/job_costing.db")
DATA_DIR = Path("data")
EXPORT_DIR = Path("exports")

STATUS_FLOW = {
    "opened": "in_progress",
    "in_progress": "completed",
    "completed": "closed",
}
ALLOWED_STATUSES = set(STATUS_FLOW.keys()) | {"closed"}


@dataclass
class CostBreakdown:
    order_id: str
    customer: str
    workshop: str
    product_type: str
    status: str
    overhead_method: str
    rule_version: str
    materials: float
    labor: float
    overhead: float
    total_cost: float
    price: float | None
    margin: float | None
    plan_cost: float | None
    variance_abs: float | None
    variance_pct: float | None


class JobOrderCostingSystem:
    def __init__(self, db_path: Path = DB_PATH) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    order_id TEXT PRIMARY KEY,
                    customer TEXT NOT NULL,
                    workshop TEXT NOT NULL,
                    product_type TEXT NOT NULL,
                    start_date TEXT NOT NULL,
                    end_date TEXT,
                    status TEXT NOT NULL CHECK(status IN ('opened','in_progress','completed','closed')),
                    price REAL,
                    plan_cost REAL,
                    overhead_method TEXT NOT NULL CHECK(overhead_method IN ('labor_hours','material_cost','fixed_rate')),
                    rule_version TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    updated_by TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS bom (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT NOT NULL,
                    material_code TEXT NOT NULL,
                    qty REAL NOT NULL CHECK(qty > 0),
                    unit_cost REAL NOT NULL CHECK(unit_cost > 0),
                    replacement_reason TEXT,
                    replacement_document TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(order_id) REFERENCES orders(order_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS labor (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT NOT NULL,
                    employee_code TEXT NOT NULL,
                    hours REAL NOT NULL CHECK(hours > 0),
                    rate REAL NOT NULL CHECK(rate > 0),
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(order_id) REFERENCES orders(order_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS overhead (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT NOT NULL,
                    amount REAL NOT NULL CHECK(amount > 0),
                    type TEXT NOT NULL,
                    department TEXT,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY(order_id) REFERENCES orders(order_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS order_cost_snapshot (
                    order_id TEXT PRIMARY KEY,
                    materials REAL NOT NULL,
                    labor REAL NOT NULL,
                    overhead REAL NOT NULL,
                    total_cost REAL NOT NULL,
                    rule_version TEXT NOT NULL,
                    finalized_at TEXT NOT NULL,
                    finalized_by TEXT NOT NULL,
                    FOREIGN KEY(order_id) REFERENCES orders(order_id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS change_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_name TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    changed_by TEXT NOT NULL,
                    changed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    details TEXT
                );

                CREATE TABLE IF NOT EXISTS error_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    error_message TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS exchange_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    direction TEXT NOT NULL,
                    dataset TEXT NOT NULL,
                    status TEXT NOT NULL,
                    message TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );
                """
            )

    def log_change(self, conn: sqlite3.Connection, entity: str, entity_id: str, op: str, user: str, details: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO change_log(entity_name, entity_id, operation, changed_by, details) VALUES(?,?,?,?,?)",
            (entity, entity_id, op, user, json.dumps(details, ensure_ascii=False)),
        )

    def create_order(
        self,
        order_id: str,
        customer: str,
        workshop: str,
        product_type: str,
        start_date: str,
        status: str,
        overhead_method: str,
        rule_version: str,
        created_by: str,
        price: float | None = None,
        plan_cost: float | None = None,
        end_date: str | None = None,
    ) -> None:
        if status not in ALLOWED_STATUSES:
            raise ValueError(f"Недопустимый статус: {status}")
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO orders(
                    order_id, customer, workshop, product_type,
                    start_date, end_date, status, price, plan_cost,
                    overhead_method, rule_version, created_by
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    order_id,
                    customer,
                    workshop,
                    product_type,
                    start_date,
                    end_date,
                    status,
                    price,
                    plan_cost,
                    overhead_method,
                    rule_version,
                    created_by,
                ),
            )
            self.log_change(conn, "orders", order_id, "insert", created_by, {"status": status})

    def update_order_status(self, order_id: str, new_status: str, user: str) -> None:
        if new_status not in ALLOWED_STATUSES:
            raise ValueError("Недопустимый новый статус")
        with self.connect() as conn:
            row = conn.execute("SELECT status FROM orders WHERE order_id=?", (order_id,)).fetchone()
            if not row:
                raise ValueError("Заказ не найден")
            old_status = row["status"]
            if old_status == "closed":
                raise ValueError("Закрытый заказ нельзя перевести в другой статус")
            expected_next = STATUS_FLOW.get(old_status)
            if new_status != expected_next:
                raise ValueError(f"Разрешён только переход {old_status} -> {expected_next}")
            if new_status in {"completed", "closed"}:
                self._validate_cost_completeness(conn, order_id)
                self.finalize_order(conn, order_id, user)
            conn.execute(
                "UPDATE orders SET status=?, updated_by=?, updated_at=CURRENT_TIMESTAMP WHERE order_id=?",
                (new_status, user, order_id),
            )
            self.log_change(conn, "orders", order_id, "status_update", user, {"from": old_status, "to": new_status})

    def _validate_cost_completeness(self, conn: sqlite3.Connection, order_id: str) -> None:
        missing = []
        for table in ("bom", "labor", "overhead"):
            count = conn.execute(f"SELECT COUNT(*) AS c FROM {table} WHERE order_id=?", (order_id,)).fetchone()["c"]
            if count == 0:
                missing.append(table)
        if missing:
            raise ValueError(f"Нельзя завершить заказ: отсутствуют строки затрат в {', '.join(missing)}")

    def finalize_order(self, conn: sqlite3.Connection, order_id: str, user: str) -> None:
        b = self.calculate_order_cost(order_id, conn=conn)
        conn.execute(
            """
            INSERT INTO order_cost_snapshot(order_id, materials, labor, overhead, total_cost, rule_version, finalized_at, finalized_by)
            VALUES(?,?,?,?,?,?,CURRENT_TIMESTAMP,?)
            ON CONFLICT(order_id) DO UPDATE SET
                materials=excluded.materials,
                labor=excluded.labor,
                overhead=excluded.overhead,
                total_cost=excluded.total_cost,
                rule_version=excluded.rule_version,
                finalized_at=CURRENT_TIMESTAMP,
                finalized_by=excluded.finalized_by
            """,
            (order_id, b.materials, b.labor, b.overhead, b.total_cost, b.rule_version, user),
        )

    def add_bom(self, order_id: str, material_code: str, qty: float, unit_cost: float, user: str, replacement_reason: str | None = None, replacement_document: str | None = None) -> None:
        with self.connect() as conn:
            self._ensure_order_editable(conn, order_id)
            conn.execute(
                """
                INSERT INTO bom(order_id, material_code, qty, unit_cost, replacement_reason, replacement_document, created_by)
                VALUES(?,?,?,?,?,?,?)
                """,
                (order_id, material_code, qty, unit_cost, replacement_reason, replacement_document, user),
            )
            self.log_change(conn, "bom", order_id, "insert", user, {"material_code": material_code, "qty": qty, "unit_cost": unit_cost})

    def add_labor(self, order_id: str, employee_code: str, hours: float, rate: float, user: str) -> None:
        with self.connect() as conn:
            self._ensure_order_editable(conn, order_id)
            conn.execute(
                "INSERT INTO labor(order_id, employee_code, hours, rate, created_by) VALUES(?,?,?,?,?)",
                (order_id, employee_code, hours, rate, user),
            )
            self.log_change(conn, "labor", order_id, "insert", user, {"employee": employee_code, "hours": hours, "rate": rate})

    def add_overhead(self, order_id: str, amount: float, type_: str, user: str, department: str | None = None) -> None:
        with self.connect() as conn:
            self._ensure_order_editable(conn, order_id)
            conn.execute(
                "INSERT INTO overhead(order_id, amount, type, department, created_by) VALUES(?,?,?,?,?)",
                (order_id, amount, type_, department, user),
            )
            self.log_change(conn, "overhead", order_id, "insert", user, {"amount": amount, "type": type_})

    def _ensure_order_editable(self, conn: sqlite3.Connection, order_id: str) -> None:
        row = conn.execute("SELECT status FROM orders WHERE order_id=?", (order_id,)).fetchone()
        if not row:
            raise ValueError("Для строки затрат не найден заказ в orders")
        if row["status"] == "closed":
            raise ValueError("Заказ в статусе closed: пересчёт запрещён, используйте корректирующий документ")

    def calculate_order_cost(self, order_id: str, conn: sqlite3.Connection | None = None) -> CostBreakdown:
        own_conn = conn is None
        if conn is None:
            conn = self.connect()
        try:
            o = conn.execute("SELECT * FROM orders WHERE order_id=?", (order_id,)).fetchone()
            if not o:
                raise ValueError("Заказ не найден")
            m = conn.execute("SELECT COALESCE(SUM(qty*unit_cost),0) AS v FROM bom WHERE order_id=?", (order_id,)).fetchone()["v"]
            l = conn.execute("SELECT COALESCE(SUM(hours*rate),0) AS v FROM labor WHERE order_id=?", (order_id,)).fetchone()["v"]
            oh = conn.execute("SELECT COALESCE(SUM(amount),0) AS v FROM overhead WHERE order_id=?", (order_id,)).fetchone()["v"]
            total = float(m + l + oh)
            price = o["price"]
            margin = None if price is None else float(price - total)
            plan = o["plan_cost"]
            variance_abs = None if plan is None else float(total - plan)
            variance_pct = None if plan in (None, 0) else float(abs(total - plan) / plan)
            return CostBreakdown(
                order_id=o["order_id"],
                customer=o["customer"],
                workshop=o["workshop"],
                product_type=o["product_type"],
                status=o["status"],
                overhead_method=o["overhead_method"],
                rule_version=o["rule_version"],
                materials=float(m),
                labor=float(l),
                overhead=float(oh),
                total_cost=total,
                price=float(price) if price is not None else None,
                margin=margin,
                plan_cost=float(plan) if plan is not None else None,
                variance_abs=variance_abs,
                variance_pct=variance_pct,
            )
        finally:
            if own_conn:
                conn.close()

    def period_report(self, date_from: str, date_to: str, workshop: str | None = None, customer: str | None = None, product_type: str | None = None, overhead_method: str | None = None) -> list[CostBreakdown]:
        sql = "SELECT order_id FROM orders WHERE start_date >= ? AND start_date <= ?"
        params: list[Any] = [date_from, date_to]
        if workshop:
            sql += " AND workshop = ?"
            params.append(workshop)
        if customer:
            sql += " AND customer = ?"
            params.append(customer)
        if product_type:
            sql += " AND product_type = ?"
            params.append(product_type)
        if overhead_method:
            sql += " AND overhead_method = ?"
            params.append(overhead_method)

        with self.connect() as conn:
            rows = conn.execute(sql, tuple(params)).fetchall()
            return [self.calculate_order_cost(r["order_id"], conn=conn) for r in rows]

    def top_n(self, n: int = 5, by: str = "margin") -> list[CostBreakdown]:
        rows = self.period_report("1900-01-01", "2999-12-31")
        if by == "overspend":
            rows.sort(key=lambda x: (x.variance_abs if x.variance_abs is not None else -10**18), reverse=True)
        else:
            rows.sort(key=lambda x: (x.margin if x.margin is not None else -10**18), reverse=True)
        return rows[:n]

    def export_report(self, rows: list[CostBreakdown], file_path: Path) -> None:
        file_path.parent.mkdir(parents=True, exist_ok=True)
        with file_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow([
                "order_id", "customer", "workshop", "product_type", "status", "overhead_method", "rule_version",
                "materials", "labor", "overhead", "total_cost", "price", "margin", "plan_cost", "variance_abs", "variance_pct"
            ])
            for r in rows:
                writer.writerow([
                    r.order_id, r.customer, r.workshop, r.product_type, r.status, r.overhead_method, r.rule_version,
                    round(r.materials, 2), round(r.labor, 2), round(r.overhead, 2), round(r.total_cost, 2),
                    "n/a" if r.price is None else round(r.price, 2),
                    "n/a" if r.margin is None else round(r.margin, 2),
                    "n/a" if r.plan_cost is None else round(r.plan_cost, 2),
                    "n/a" if r.variance_abs is None else round(r.variance_abs, 2),
                    "n/a" if r.variance_pct is None else round(r.variance_pct, 4),
                ])

    def import_csv_folder(self, folder: Path) -> None:
        backup = self.db_path.with_suffix(".bak")
        if self.db_path.exists():
            shutil.copy2(self.db_path, backup)
        with self.connect() as conn:
            try:
                conn.execute("BEGIN")
                for table, cols in {
                    "orders": ["order_id", "customer", "workshop", "product_type", "start_date", "end_date", "status", "price", "plan_cost", "overhead_method", "rule_version", "created_by"],
                    "bom": ["order_id", "material_code", "qty", "unit_cost", "replacement_reason", "replacement_document", "created_by"],
                    "labor": ["order_id", "employee_code", "hours", "rate", "created_by"],
                    "overhead": ["order_id", "amount", "type", "department", "created_by"],
                }.items():
                    p = folder / f"{table}.csv"
                    if not p.exists():
                        continue
                    placeholders = ",".join(["?"] * len(cols))
                    with p.open("r", encoding="utf-8") as f:
                        for row in csv.DictReader(f):
                            conn.execute(f"INSERT INTO {table}({','.join(cols)}) VALUES({placeholders})", tuple(row.get(c) or None for c in cols))
                conn.commit()
                conn.execute("INSERT INTO exchange_log(direction,dataset,status,message) VALUES('import','csv','ok','import completed')")
            except Exception as exc:
                conn.rollback()
                conn.execute("INSERT INTO error_log(source,error_message) VALUES('import_csv_folder',?)", (str(exc),))
                conn.execute("INSERT INTO exchange_log(direction,dataset,status,message) VALUES('import','csv','failed',?)", (str(exc),))
                if backup.exists():
                    shutil.copy2(backup, self.db_path)
                raise

    def seed_synthetic(self) -> None:
        self.init_db()
        with self.connect() as conn:
            if conn.execute("SELECT COUNT(*) c FROM orders").fetchone()["c"] > 0:
                return
        orders = [
            ("ORD-001", "АО Север", "Цех-1", "Изделие-A", "2026-01-10", "opened", "labor_hours", "ovh-v1", "planner", 150000.0, 132000.0),
            ("ORD-002", "ООО Маяк", "Цех-2", "Изделие-B", "2026-02-03", "in_progress", "material_cost", "ovh-v1", "planner", 220000.0, 205000.0),
            ("ORD-003", "ЗАО Вектор", "Цех-1", "Изделие-C", "2026-03-01", "in_progress", "fixed_rate", "ovh-v2", "planner", None, 98000.0),
        ]
        for o in orders:
            self.create_order(
                order_id=o[0], customer=o[1], workshop=o[2], product_type=o[3],
                start_date=o[4], status=o[5], overhead_method=o[6], rule_version=o[7],
                created_by=o[8], price=o[9], plan_cost=o[10]
            )

        # >= 100 cost rows
        for i in range(1, 51):
            oid = "ORD-001" if i <= 18 else "ORD-002" if i <= 35 else "ORD-003"
            self.add_bom(oid, f"MAT-{i:03}", qty=1 + (i % 5), unit_cost=80 + i * 2, user="shop_user")
        for i in range(1, 36):
            oid = "ORD-001" if i <= 12 else "ORD-002" if i <= 25 else "ORD-003"
            self.add_labor(oid, f"EMP-{i:03}", hours=2 + (i % 7), rate=450 + i * 5, user="foreman")
        for i in range(1, 26):
            oid = "ORD-001" if i <= 9 else "ORD-002" if i <= 18 else "ORD-003"
            self.add_overhead(oid, amount=300 + i * 11, type_="energy" if i % 2 else "maintenance", user="fin_controller", department="D1" if i % 2 else "D2")

    def order_form(self, order_id: str) -> dict[str, Any]:
        with self.connect() as conn:
            o = conn.execute("SELECT * FROM orders WHERE order_id=?", (order_id,)).fetchone()
            if not o:
                raise ValueError("Заказ не найден")
            bom = [dict(r) for r in conn.execute("SELECT material_code, qty, unit_cost, replacement_reason, replacement_document FROM bom WHERE order_id=?", (order_id,)).fetchall()]
            labor = [dict(r) for r in conn.execute("SELECT employee_code, hours, rate FROM labor WHERE order_id=?", (order_id,)).fetchall()]
            ovh = [dict(r) for r in conn.execute("SELECT amount, type, department FROM overhead WHERE order_id=?", (order_id,)).fetchall()]
        return {
            "order": dict(o),
            "materials": bom,
            "labor": labor,
            "overhead": ovh,
            "methodology": {
                "formula": "cost = sum(qty*unit_cost) + sum(hours*rate) + sum(overhead)",
                "rule_version": o["rule_version"],
                "price_source": "order.price",
            },
        }


def print_order_report(b: CostBreakdown) -> None:
    def fmt(v: float | None) -> str:
        return "n/a" if v is None else f"{v:,.2f}"

    print(f"Заказ: {b.order_id} | Клиент: {b.customer} | Статус: {b.status}")
    print(f"Подразделение: {b.workshop} | Тип изделия: {b.product_type}")
    print(f"Метод накладных: {b.overhead_method} | Версия правил: {b.rule_version}")
    print("-" * 72)
    print(f"Материалы: {fmt(b.materials)}")
    print(f"Труд:      {fmt(b.labor)}")
    print(f"Накладные: {fmt(b.overhead)}")
    print(f"ИТОГО:     {fmt(b.total_cost)}")
    print(f"Цена:      {fmt(b.price)}")
    print(f"Маржа:     {fmt(b.margin)}")
    print(f"План:      {fmt(b.plan_cost)}")
    print(f"Откл.:     {fmt(b.variance_abs)} ({'n/a' if b.variance_pct is None else f'{b.variance_pct:.2%}'})")


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Job-order costing demo")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init-db")
    sub.add_parser("seed")

    r1 = sub.add_parser("report-order")
    r1.add_argument("--order-id", required=True)

    r2 = sub.add_parser("report-period")
    r2.add_argument("--date-from", required=True)
    r2.add_argument("--date-to", required=True)
    r2.add_argument("--workshop")
    r2.add_argument("--customer")
    r2.add_argument("--product-type")
    r2.add_argument("--overhead-method")

    e = sub.add_parser("export-csv")
    e.add_argument("--date-from", required=True)
    e.add_argument("--date-to", required=True)
    e.add_argument("--out", default=str(EXPORT_DIR / "cost_report.csv"))

    t = sub.add_parser("top")
    t.add_argument("--by", choices=["margin", "overspend"], default="margin")
    t.add_argument("--n", type=int, default=5)

    f = sub.add_parser("form")
    f.add_argument("--order-id", required=True)

    i = sub.add_parser("import-csv")
    i.add_argument("--folder", required=True)

    return p


def main() -> None:
    args = build_parser().parse_args()
    app = JobOrderCostingSystem()

    if args.cmd == "init-db":
        app.init_db()
        print("DB initialized")
    elif args.cmd == "seed":
        app.seed_synthetic()
        print("Synthetic dataset loaded")
    elif args.cmd == "report-order":
        print_order_report(app.calculate_order_cost(args.order_id))
    elif args.cmd == "report-period":
        rows = app.period_report(args.date_from, args.date_to, args.workshop, args.customer, args.product_type, args.overhead_method)
        for r in rows:
            print_order_report(r)
            print()
    elif args.cmd == "export-csv":
        rows = app.period_report(args.date_from, args.date_to)
        app.export_report(rows, Path(args.out))
        print(f"Exported: {args.out}")
    elif args.cmd == "top":
        for r in app.top_n(args.n, args.by):
            print(f"{r.order_id}: margin={r.margin}, overspend={r.variance_abs}")
    elif args.cmd == "form":
        print(json.dumps(app.order_form(args.order_id), ensure_ascii=False, indent=2))
    elif args.cmd == "import-csv":
        app.import_csv_folder(Path(args.folder))
        print("Import done")


if __name__ == "__main__":
    main()
