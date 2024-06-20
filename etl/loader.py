"""Carga de eventos payments.completed al warehouse.

Loader trabaja contra cualquier objeto que exponga la interfaz de conexión/cursor
de psycopg (`conn.cursor()`, `cur.execute(sql, params)`, `cur.fetchone()`,
`conn.commit()`) para poder testear con un fake en memoria (ver tests/test_loader.py)
sin levantar Postgres.
"""

from __future__ import annotations

import logging
from decimal import Decimal
from pathlib import Path
from typing import Any

logger = logging.getLogger("reporting-etl.loader")

_INIT_SQL_PATH = Path(__file__).resolve().parent.parent / "sql" / "001_init.sql"

_UPSERT_FACT_PAYMENTS = """
    INSERT INTO fact_payments
        (payment_id, from_account, to_account, merchant_id, amount, currency, occurred_at)
    VALUES (%(payment_id)s, %(from_account)s, %(to_account)s, %(merchant_id)s,
            %(amount)s, %(currency)s, %(occurred_at)s)
    ON CONFLICT (payment_id) DO NOTHING
    RETURNING payment_id
"""

_UPSERT_DAILY_VOLUME = """
    INSERT INTO daily_volume (day, currency, total, count)
    VALUES (%(day)s, %(currency)s, %(amount)s, 1)
    ON CONFLICT (day, currency) DO UPDATE
        SET total = daily_volume.total + EXCLUDED.total,
            count = daily_volume.count + 1
"""


def init_schema(conn: Any) -> None:
    """Aplica sql/001_init.sql. Idempotente (CREATE TABLE IF NOT EXISTS)."""
    sql = _INIT_SQL_PATH.read_text()
    cur = conn.cursor()
    cur.execute(sql)
    conn.commit()


class Loader:
    """Aplica un evento payments.completed al warehouse de forma idempotente."""

    def __init__(self, conn: Any):
        self._conn = conn

    def load_event(self, event: dict) -> bool:
        """Carga un evento. Devuelve True si era nuevo, False si era un duplicado.

        Idempotencia: fact_payments tiene PK en payment_id; el INSERT ON CONFLICT
        DO NOTHING ... RETURNING nos dice si insertó una fila nueva. daily_volume
        solo se actualiza cuando el evento era nuevo, así el mismo evento
        reprocesado (mismo payment_id) no vuelve a sumar amount ni a incrementar
        count.
        """
        payment_id = event["payment_id"]
        amount = Decimal(str(event["amount"]))
        occurred_at = event["occurred_at"]
        day = _to_date(occurred_at)

        cur = self._conn.cursor()
        cur.execute(
            _UPSERT_FACT_PAYMENTS,
            {
                "payment_id": payment_id,
                "from_account": event["from_account"],
                "to_account": event["to_account"],
                "merchant_id": event.get("merchant_id"),
                "amount": amount,
                "currency": event["currency"],
                "occurred_at": occurred_at,
            },
        )
        inserted = cur.fetchone()

        if inserted is None:
            logger.info("evento duplicado, no se re-acumula", extra={"payment_id": payment_id})
            self._conn.commit()
            return False

        cur.execute(
            _UPSERT_DAILY_VOLUME,
            {"day": day, "currency": event["currency"], "amount": amount},
        )
        self._conn.commit()
        return True


def _to_date(occurred_at: Any) -> Any:
    """occurred_at llega como str ISO 8601 (las convenciones internas de API) o como date/datetime."""
    if hasattr(occurred_at, "date"):
        return occurred_at.date()
    # string ISO 8601, ej "2026-09-09T12:00:00Z"
    return occurred_at[:10]
