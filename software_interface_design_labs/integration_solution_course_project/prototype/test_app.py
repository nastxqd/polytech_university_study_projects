import os
import json
import threading
import unittest
from pathlib import Path
from http.server import ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request, urlopen

TEST_DB = Path(__file__).with_name(".test-integration.db")
os.environ["INTEGRATION_DB"] = str(TEST_DB)

import app


class IntegrationFlowTest(unittest.TestCase):
    def setUp(self):
        TEST_DB.unlink(missing_ok=True)
        app.initialize()

    def test_success_reserves_stock_and_updates_crm(self):
        result, replayed = app.create_order({"customerId": "C-1", "items": [{"sku": "SKU-MUG", "quantity": 2}]}, "key-1")
        self.assertFalse(replayed)
        self.assertEqual(result["status"], "confirmed")
        with app.connection() as con:
            self.assertEqual(con.execute("SELECT available FROM inventory WHERE sku='SKU-MUG'").fetchone()[0], 28)
            self.assertEqual(con.execute("SELECT last_order_id FROM crm_customers WHERE customer_id='C-1'").fetchone()[0], result["id"])

    def test_insufficient_stock_does_not_create_order(self):
        with self.assertRaises(app.DomainError) as err:
            app.create_order({"customerId": "C-1", "items": [{"sku": "SKU-CHAIR", "quantity": 99}]}, "key-2")
        self.assertEqual(err.exception.code, "OUT_OF_STOCK")
        with app.connection() as con:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM orders").fetchone()[0], 0)

    def test_payment_decline_releases_reserved_stock(self):
        result, _ = app.create_order({"customerId": "C-2", "items": [{"sku": "SKU-LAMP", "quantity": 1}], "paymentApproved": False}, "key-3")
        self.assertEqual(result["status"], "cancelled")
        with app.connection() as con:
            self.assertEqual(con.execute("SELECT available FROM inventory WHERE sku='SKU-LAMP'").fetchone()[0], 8)
            self.assertEqual(con.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 0)

    def test_idempotent_replay_returns_existing_order(self):
        payload = {"customerId": "C-3", "items": [{"sku": "SKU-MUG", "quantity": 1}]}
        first, replayed_first = app.create_order(payload, "same-key")
        second, replayed_second = app.create_order(payload, "same-key")
        self.assertFalse(replayed_first)
        self.assertTrue(replayed_second)
        self.assertEqual(first["id"], second["id"])
        with app.connection() as con:
            self.assertEqual(con.execute("SELECT available FROM inventory WHERE sku='SKU-MUG'").fetchone()[0], 29)


class HttpContractTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), app.Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base_url = f"http://127.0.0.1:{cls.server.server_address[1]}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=2)

    def setUp(self):
        TEST_DB.unlink(missing_ok=True)
        app.initialize()

    def post_order(self, payload, key):
        request = Request(
            self.base_url + "/orders",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json", "Idempotency-Key": key},
            method="POST",
        )
        try:
            response = urlopen(request, timeout=3)
        except HTTPError as error:
            response = error
        return response.status, response.headers.get("Content-Type"), json.loads(response.read())

    def test_http_json_round_trip_and_idempotent_replay(self):
        payload = {"customerId": "C-HTTP", "items": [{"sku": "SKU-LAMP", "quantity": 2}]}
        status, content_type, first = self.post_order(payload, "http-key")
        self.assertEqual(status, 201)
        self.assertIn("application/json", content_type)
        self.assertEqual(first["status"], "confirmed")
        self.assertEqual(first["totalMinor"], 490000)
        replay_status, _, replay = self.post_order(payload, "http-key")
        self.assertEqual(replay_status, 200)
        self.assertEqual(replay["id"], first["id"])

        with urlopen(self.base_url + "/orders/" + first["id"], timeout=3) as response:
            received = json.loads(response.read())
        self.assertEqual(received["items"][0]["sku"], "SKU-LAMP")
        self.assertEqual(received["items"][0]["quantity"], 2)

    def test_http_stock_shortage_returns_json_error(self):
        status, content_type, body = self.post_order(
            {"customerId": "C-HTTP", "items": [{"sku": "SKU-CHAIR", "quantity": 99}]},
            "shortage-key",
        )
        self.assertEqual(status, 409)
        self.assertIn("application/json", content_type)
        self.assertEqual(body["error"]["code"], "OUT_OF_STOCK")

    def test_http_payment_decline_returns_cancelled_order(self):
        status, _, body = self.post_order(
            {"customerId": "C-HTTP", "items": [{"sku": "SKU-LAMP", "quantity": 1}], "paymentApproved": False},
            "declined-key",
        )
        self.assertEqual(status, 402)
        self.assertEqual(body["status"], "cancelled")
        with app.connection() as con:
            self.assertEqual(con.execute("SELECT available FROM inventory WHERE sku='SKU-LAMP'").fetchone()[0], 8)


if __name__ == "__main__":
    unittest.main(verbosity=2)
