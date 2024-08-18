"""Consumidor Kafka de `<KAFKA_TOPIC_PREFIX>payments.completed` (las convenciones internas de API §7).

Elegimos confluent-kafka (librdkafka) sobre aiokafka: el resto del ETL es sincrónico
(psycopg sync, http.server en un thread) y confluent-kafka nos da commits manuales
sencillos para no perder eventos si el load a Postgres falla.
"""

from __future__ import annotations

import json
import logging

from confluent_kafka import Consumer

from etl.loader import Loader

logger = logging.getLogger("reporting-etl.consumer")


def build_consumer(bootstrap_servers: str, group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def run(consumer: Consumer, topic: str, loader: Loader, metrics) -> None:
    """Loop principal: poll, cargar, commitear. Corre hasta que el consumer se cierre."""
    consumer.subscribe([topic])
    logger.info("suscripto a %s", topic)
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("error del consumer: %s", msg.error())
                continue

            event = json.loads(msg.value())
            is_new = loader.load_event(event)
            if is_new:
                metrics.events_processed_total.inc()
            else:
                metrics.events_duplicate_total.inc()

            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
