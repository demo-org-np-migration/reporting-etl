"""Servidor HTTP mínimo (/health, /metrics) en un thread aparte, sin bloquear el consumer.

las convenciones internas de API §1: todo servicio escucha en 8080; /health -> 200 {"status":"ok"};
/metrics en formato Prometheus.
"""

from __future__ import annotations

import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from prometheus_client import CONTENT_TYPE_LATEST, Counter, generate_latest

logger = logging.getLogger("reporting-etl.metrics")

events_processed_total = Counter(
    "reporting_etl_events_processed_total",
    "Eventos payments.completed procesados y cargados al warehouse",
)
events_duplicate_total = Counter(
    "reporting_etl_events_duplicate_total",
    "Eventos payments.completed recibidos que ya estaban en fact_payments",
)


class _Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # silencia el log default de http.server
        logger.debug("%s - %s", self.address_string(), fmt % args)

    def do_GET(self):
        if self.path == "/health":
            body = b'{"status":"ok"}'
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/metrics":
            body = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(404)
            self.end_headers()


def start_http_server(port: int) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer(("0.0.0.0", port), _Handler)
    thread = threading.Thread(target=server.serve_forever, name="http-server", daemon=True)
    thread.start()
    logger.info("http server escuchando en :%d (/health, /metrics)", port)
    return server
