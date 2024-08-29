"""Tests de etl/loader.py contra una conexión fake en memoria (sin Postgres real)."""

from decimal import Decimal

from etl.loader import Loader

EVENT = {
    "event": "payments.completed",
    "version": 1,
    "payment_id": "11111111-1111-4111-8111-111111111111",
    "from_account": "aaaaaaaa-0000-4000-8000-000000000101",
    "to_account": "bbbbbbbb-0000-4000-8000-000000000201",
    "merchant_id": None,
    "amount": "1250.50",
    "currency": "ARS",
    "occurred_at": "2026-09-09T12:00:00Z",
}


class FakeCursor:
    """Interpreta las dos queries que emite Loader contra dicts en memoria."""

    def __init__(self, db: dict):
        self._db = db
        self._last_result = None

    def execute(self, sql: str, params: dict | None = None):
        sql_norm = " ".join(sql.split())
        params = params or {}

        if sql_norm.startswith("INSERT INTO fact_payments"):
            fact_payments = self._db["fact_payments"]
            payment_id = params["payment_id"]
            if payment_id in fact_payments:
                self._last_result = None  # ON CONFLICT DO NOTHING -> no RETURNING row
            else:
                fact_payments[payment_id] = dict(params)
                self._last_result = (payment_id,)
        elif sql_norm.startswith("INSERT INTO daily_volume"):
            daily_volume = self._db["daily_volume"]
            key = (params["day"], params["currency"])
            amount = Decimal(str(params["amount"]))
            if key in daily_volume:
                daily_volume[key]["total"] += amount
                daily_volume[key]["count"] += 1
            else:
                daily_volume[key] = {"total": amount, "count": 1}
            self._last_result = None
        else:
            raise AssertionError(f"query no esperada en el fake: {sql_norm[:50]!r}")

    def fetchone(self):
        return self._last_result


class FakeConnection:
    def __init__(self):
        self.db = {"fact_payments": {}, "daily_volume": {}}
        self.commits = 0

    def cursor(self):
        return FakeCursor(self.db)

    def commit(self):
        self.commits += 1


def test_load_event_carga_fact_payments_y_daily_volume():
    conn = FakeConnection()
    loader = Loader(conn)

    is_new = loader.load_event(EVENT)

    assert is_new is True
    assert EVENT["payment_id"] in conn.db["fact_payments"]
    key = ("2026-09-09", "ARS")
    assert conn.db["daily_volume"][key]["total"] == Decimal("1250.50")
    assert conn.db["daily_volume"][key]["count"] == 1


def test_load_event_duplicado_no_duplica_ni_reacumula():
    conn = FakeConnection()
    loader = Loader(conn)

    first = loader.load_event(EVENT)
    second = loader.load_event(EVENT)

    assert first is True
    assert second is False
    assert len(conn.db["fact_payments"]) == 1
    key = ("2026-09-09", "ARS")
    # el segundo intento no debe volver a sumar amount ni a incrementar count
    assert conn.db["daily_volume"][key]["total"] == Decimal("1250.50")
    assert conn.db["daily_volume"][key]["count"] == 1
