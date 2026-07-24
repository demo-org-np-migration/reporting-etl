"""Consumidor Kafka de `<KAFKA_TOPIC_PREFIX>payments.completed` (las convenciones internas de API §7).

Elegimos confluent-kafka (librdkafka) sobre aiokafka: el resto del ETL es sincrónico
(psycopg sync, http.server en un thread) y confluent-kafka nos da commits manuales
sencillos para no perder eventos si el load a Postgres falla.
"""

from __future__ import annotations

import json
import logging
import time

from confluent_kafka import Consumer

from etl.loader import Loader

logger = logging.getLogger("reporting-etl.consumer")

# Pausa antes de reintentar tras un error transitorio (DB caída, etc.) para no
# entrar en busy loop pegando el mismo mensaje una y otra vez.
_RETRY_DELAY_SECONDS = 1


def build_consumer(bootstrap_servers: str, group_id: str) -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )


def process_message(msg, loader: Loader, metrics) -> bool:
    """Procesa un mensaje ya sin error de Kafka (`msg.error()` es None).

    Devuelve True si hay que commitear el offset (se cargó bien, era un duplicado,
    o el mensaje estaba corrupto y lo descartamos) y False si hay que reintentar en
    el próximo poll (error transitorio, típicamente Postgres caído): no commiteamos
    para no perder el evento.
    """
    try:
        event = json.loads(msg.value())
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        logger.error("mensaje malformado en offset %s, se descarta: %s", msg.offset(), exc)
        return True

    try:
        is_new = loader.load_event(event)
    except Exception:
        logger.exception("error cargando el evento en offset %s, se reintenta", msg.offset())
        return False

    if is_new:
        metrics.events_processed_total.inc()
    else:
        metrics.events_duplicate_total.inc()
    return True


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

            should_commit = process_message(msg, loader, metrics)
            if should_commit:
                consumer.commit(message=msg, asynchronous=False)
            else:
                # No commiteamos: el mensaje se re-entrega en el próximo poll.
                # El sleep evita un busy loop pegando el mismo error sin parar.
                time.sleep(_RETRY_DELAY_SECONDS)
    finally:
        consumer.close()
