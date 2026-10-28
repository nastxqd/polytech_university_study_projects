"""Учебный интеграционный сервис интернет-магазина на стандартной библиотеке Python."""
from __future__ import annotations

import json
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

DB_PATH = Path(os.environ.get("INTEGRATION_DB", Path(__file__).with_name("integration.db")))


def connect():
    con = sqlite3.connect(DB_PATH, timeout=5)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys = ON")
    return con


@contextmanager
def connection():
    con = connect()
    try:
        with con:
            yield con
    finally:
        con.close()


def initialize():
    with connection() as con:
        con.executescript("""
        CREATE TABLE IF NOT EXISTS inventory(sku TEXT PRIMARY KEY, name TEXT NOT NULL, available INTEGER NOT NULL CHECK(available >= 0));
        CREATE TABLE IF NOT EXISTS orders(id TEXT PRIMARY KEY, customer_id TEXT NOT NULL, status TEXT NOT NULL, total_minor INTEGER NOT NULL, currency TEXT NOT NULL, created_at TEXT NOT NULL, idempotency_key TEXT UNIQUE NOT NULL);
        CREATE TABLE IF NOT EXISTS order_items(order_id TEXT NOT NULL REFERENCES orders(id), sku TEXT NOT NULL, quantity INTEGER NOT NULL, unit_price_minor INTEGER NOT NULL, PRIMARY KEY(order_id, sku));
        CREATE TABLE IF NOT EXISTS payments(id TEXT PRIMARY KEY, order_id TEXT NOT NULL UNIQUE REFERENCES orders(id), status TEXT NOT NULL, amount_minor INTEGER NOT NULL, provider_ref TEXT);
        CREATE TABLE IF NOT EXISTS crm_customers(customer_id TEXT PRIMARY KEY, last_order_id TEXT, last_order_status TEXT);
        CREATE TABLE IF NOT EXISTS outbox(id TEXT PRIMARY KEY, event_type TEXT NOT NULL, aggregate_id TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL, published_at TEXT);
        CREATE TABLE IF NOT EXISTS inbox(consumer TEXT NOT NULL, event_id TEXT NOT NULL, processed_at TEXT NOT NULL, PRIMARY KEY(consumer, event_id));
        """)
        con.executemany("INSERT OR IGNORE INTO inventory(sku,name,available) VALUES(?,?,?)", [
            ("SKU-CHAIR", "Стул офисный", 12), ("SKU-LAMP", "Настольная лампа", 8), ("SKU-MUG", "Кружка", 30)])


def now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class DomainError(Exception):
    def __init__(self, code, message, status=422, details=None):
        super().__init__(message)
        self.code, self.status, self.details = code, status, details or {}


def process_pending_events(limit=100):
    """Идемпотентный CRM-консьюмер. Неподтверждённое событие остаётся в outbox."""
    processed = 0
    with connection() as con:
        rows = con.execute("SELECT * FROM outbox WHERE published_at IS NULL ORDER BY created_at LIMIT ?", (limit,)).fetchall()
        for event in rows:
            if event["event_type"] != "OrderConfirmed":
                continue
            inserted = con.execute("INSERT OR IGNORE INTO inbox(consumer,event_id,processed_at) VALUES('crm',?,?)", (event["id"], now())).rowcount
            if inserted:
                data = json.loads(event["payload"])
                con.execute("INSERT INTO crm_customers(customer_id,last_order_id,last_order_status) VALUES(?,?,?) ON CONFLICT(customer_id) DO UPDATE SET last_order_id=excluded.last_order_id,last_order_status=excluded.last_order_status", (data["customerId"], data["orderId"], "confirmed"))
            con.execute("UPDATE outbox SET published_at=? WHERE id=?", (now(), event["id"]))
            processed += 1
    return processed


def create_order(data, idempotency_key):
    if not idempotency_key:
        raise DomainError("IDEMPOTENCY_KEY_REQUIRED", "Передайте заголовок Idempotency-Key", 400)
    customer = data.get("customerId")
    items = data.get("items")
    if not isinstance(customer, str) or not customer.strip() or not isinstance(items, list) or not items:
        raise DomainError("INVALID_ORDER", "Нужны customerId и непустой массив items")
    normalized = []
    for item in items:
        sku, qty = item.get("sku"), item.get("quantity")
        if not isinstance(sku, str) or not isinstance(qty, int) or isinstance(qty, bool) or qty < 1:
            raise DomainError("INVALID_ITEM", "Каждая позиция должна содержать sku и целое quantity > 0")
        normalized.append((sku, qty))
    with connection() as con:
        previous = con.execute("SELECT id,status,total_minor,currency FROM orders WHERE idempotency_key=?", (idempotency_key,)).fetchone()
        if previous:
            return dict(previous), True
        try:
            total = 0
            prices = {"SKU-CHAIR": 850000, "SKU-LAMP": 245000, "SKU-MUG": 39000}
            for sku, qty in normalized:
                stock = con.execute("SELECT available FROM inventory WHERE sku=?", (sku,)).fetchone()
                if stock is None:
                    raise DomainError("UNKNOWN_SKU", f"Неизвестный артикул {sku}", 422, {"sku": sku})
                if stock["available"] < qty:
                    raise DomainError("OUT_OF_STOCK", f"Недостаточно товара {sku}", 409, {"sku": sku, "available": stock["available"], "requested": qty})
                total += prices[sku] * qty
            order_id = str(uuid.uuid4())
            # Резервирование склада и заказ фиксируются одной транзакцией.
            con.execute("INSERT INTO orders VALUES(?,?,?,?,?,?,?)", (order_id, customer, "awaiting_payment", total, "RUB", now(), idempotency_key))
            for sku, qty in normalized:
                con.execute("UPDATE inventory SET available=available-? WHERE sku=?", (qty, sku))
                con.execute("INSERT INTO order_items VALUES(?,?,?,?)", (order_id, sku, qty, prices[sku]))
            approved = data.get("paymentApproved", True)
            payment_id = str(uuid.uuid4())
            if not approved:
                # Отказ оплаты: заказ отменяется, резерв возвращается в той же транзакции.
                con.execute("INSERT INTO payments VALUES(?,?,?,?,?)", (payment_id, order_id, "declined", total, None))
                con.execute("UPDATE orders SET status='cancelled' WHERE id=?", (order_id,))
                for sku, qty in normalized:
                    con.execute("UPDATE inventory SET available=available+? WHERE sku=?", (qty, sku))
                result = {"id": order_id, "status": "cancelled", "totalMinor": total, "currency": "RUB", "reason": "PAYMENT_DECLINED"}
            else:
                con.execute("INSERT INTO payments VALUES(?,?,?,?,?)", (payment_id, order_id, "captured", total, "SIM-" + payment_id[:8]))
                con.execute("UPDATE orders SET status='confirmed' WHERE id=?", (order_id,))
                payload = json.dumps({"orderId": order_id, "customerId": customer, "totalMinor": total, "currency": "RUB"}, ensure_ascii=False)
                con.execute("INSERT INTO outbox VALUES(?,?,?,?,?,NULL)", (str(uuid.uuid4()), "OrderConfirmed", order_id, payload, now()))
                result = {"id": order_id, "status": "confirmed", "totalMinor": total, "currency": "RUB"}
        except Exception:
            raise
    if result["status"] == "confirmed":
        process_pending_events()
    return result, False


class Handler(BaseHTTPRequestHandler):
    server_version = "CourseIntegration/1.0"

    def log_message(self, fmt, *args):
        print(json.dumps({"level": "INFO", "message": fmt % args}, ensure_ascii=False))

    def send_json(self, status, obj):
        raw = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        path = urlparse(self.path).path
        if path == "/health":
            return self.send_json(200, {"status": "ok"})
        if path.startswith("/orders/"):
            order_id = path.rsplit("/", 1)[-1]
            with connection() as con:
                row = con.execute("SELECT id,customer_id,status,total_minor,currency,created_at FROM orders WHERE id=?", (order_id,)).fetchone()
                if not row:
                    return self.send_json(404, {"error": {"code": "ORDER_NOT_FOUND", "message": "Заказ не найден"}})
                items = con.execute("SELECT sku,quantity,unit_price_minor FROM order_items WHERE order_id=?", (order_id,)).fetchall()
            return self.send_json(200, {"id": row["id"], "customerId": row["customer_id"], "status": row["status"], "totalMinor": row["total_minor"], "currency": row["currency"], "createdAt": row["created_at"], "items": [dict(x) for x in items]})
        return self.send_json(404, {"error": {"code": "NOT_FOUND", "message": "Маршрут не найден"}})

    def do_POST(self):
        if urlparse(self.path).path != "/orders":
            return self.send_json(404, {"error": {"code": "NOT_FOUND", "message": "Маршрут не найден"}})
        try:
            length = int(self.headers.get("Content-Length", "0"))
            if length > 100_000:
                raise DomainError("PAYLOAD_TOO_LARGE", "Размер запроса превышает 100 КБ", 413)
            data = json.loads(self.rfile.read(length))
            result, replayed = create_order(data, self.headers.get("Idempotency-Key"))
            result["replayed"] = replayed
            return self.send_json(200 if replayed else (201 if result["status"] == "confirmed" else 402), result)
        except json.JSONDecodeError:
            return self.send_json(400, {"error": {"code": "INVALID_JSON", "message": "Тело запроса должно быть JSON"}})
        except DomainError as e:
            return self.send_json(e.status, {"error": {"code": e.code, "message": str(e), "details": e.details}})

    def do_OPTIONS(self):
        self.send_response(204); self.send_header("Allow", "GET, POST, OPTIONS"); self.end_headers()


def main():
    initialize()
    host, port = os.environ.get("INTEGRATION_HOST", "127.0.0.1"), int(os.environ.get("INTEGRATION_PORT", "8080"))
    print(f"Интеграционный прототип запущен на http://{host}:{port}")
    ThreadingHTTPServer((host, port), Handler).serve_forever()


if __name__ == "__main__":
    main()
