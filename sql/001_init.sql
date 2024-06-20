-- warehouse: dueño reporting-etl (las convenciones internas de API §5)
-- Aplicado al arrancar el proceso (etl/loader.py:init_schema), sin herramienta de migraciones.

CREATE TABLE IF NOT EXISTS fact_payments (
    payment_id   uuid PRIMARY KEY,
    from_account uuid NOT NULL,
    to_account   uuid NOT NULL,
    merchant_id  uuid NULL,
    amount       numeric(18,2) NOT NULL,
    currency     char(3) NOT NULL,
    occurred_at  timestamptz NOT NULL,
    loaded_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS daily_volume (
    day      date NOT NULL,
    currency char(3) NOT NULL,
    total    numeric(18,2) NOT NULL DEFAULT 0,
    count    int NOT NULL DEFAULT 0,
    PRIMARY KEY (day, currency)
);
