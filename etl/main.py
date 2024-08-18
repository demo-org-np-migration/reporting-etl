"""Entry point de reporting-etl: init schema, arranca /health+/metrics, consume Kafka."""

from __future__ import annotations

import json
import logging
import signal
import sys

import psycopg

from etl import consumer as consumer_mod
from etl import metrics
from etl.loader import Loader, init_schema
from etl.settings import get_settings


def _configure_logging(level: str, service_name: str, env: str) -> None:
    class JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            payload = {
                "level": record.levelname,
                "msg": record.getMessage(),
                "service": service_name,
                "env": env,
            }
            return json.dumps(payload)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)


def main() -> None:
    settings = get_settings()
    _configure_logging(settings.log_level, settings.service_name, settings.env)
    logger = logging.getLogger("reporting-etl.main")
    logger.info("arrancando reporting-etl env=%s", settings.env)

    metrics.start_http_server(settings.port)

    conn = psycopg.connect(settings.dsn, autocommit=False)
    init_schema(conn)
    loader = Loader(conn)

    kafka_consumer = consumer_mod.build_consumer(settings.kafka_bootstrap, settings.kafka_group_id)

    def _shutdown(signum, frame):
        logger.info("señal %s recibida, cerrando", signum)
        kafka_consumer.close()
        conn.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    consumer_mod.run(kafka_consumer, settings.payments_completed_topic, loader, metrics)


if __name__ == "__main__":
    main()
