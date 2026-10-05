"""Métriques Prometheus et sondes HTTP des drivers Spark Structured Streaming (Level 4).

Le driver expose, sur METRICS_PORT (défaut 9108) :
  /metrics  format Prometheus (scrapé grâce aux annotations prometheus.io/* du pod)
  /healthz  liveness  : 500 si une requête s'est arrêtée en erreur ou ne progresse plus
  /ready    readiness : 200 quand toutes les requêtes tournent et ont progressé récemment

Les valeurs viennent d'un StreamingQueryListener (progression de chaque micro-lot,
y compris les événements « idle » émis toutes les 10 s sans donnée) et des callbacks
foreachBatch (lag par pays, DLQ, alertes).

Lag Kafka d'une requête = Σ (dernier offset du topic − dernier offset traité) sur les
partitions : l'équivalent du « consumer lag », que Spark ne publie pas dans un consumer
group Kafka (il garde ses offsets dans le checkpoint).
"""
from __future__ import annotations

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

try:
    from prometheus_client import (CONTENT_TYPE_LATEST, CollectorRegistry, Counter, Gauge,
                                   generate_latest)
    HAVE_PROM = True
except ImportError:  # dépendance optionnelle (docker compose des levels 1-3)
    HAVE_PROM = False

READY_MAX_AGE_S = 120      # readiness : progression depuis moins de 2 min
LIVE_MAX_AGE_S = 600       # liveness  : au-delà de 10 min sans progression -> redémarrage


def _offsets(value) -> dict:
    """endOffset / latestOffset d'une source Kafka : objet JSON ou chaîne JSON."""
    if value is None:
        return {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except ValueError:
            return {}
    return value if isinstance(value, dict) else {}


def kafka_lag(source: dict) -> dict[str, int]:
    """Lag par topic d'une source de la progression d'une requête."""
    end, latest = _offsets(source.get("endOffset")), _offsets(source.get("latestOffset"))
    lag: dict[str, int] = {}
    for topic, parts in latest.items():
        if not isinstance(parts, dict):
            continue
        done = end.get(topic, {}) if isinstance(end.get(topic), dict) else {}
        lag[topic] = int(sum(max(int(off) - int(done.get(p, 0)), 0)
                             for p, off in parts.items()))
    return lag


class _Noop:
    def labels(self, *a, **k):
        return self

    def set(self, *a):
        pass

    def inc(self, *a):
        pass


class StreamMetrics:
    def __init__(self, job: str, registry=None):
        self.job = job
        self.queries: dict[str, dict] = {}      # nom -> {"last": ts, "active": bool}
        self._names: dict[str, str] = {}        # id de requête -> nom
        self.failed: str | None = None
        self._lock = threading.Lock()
        if HAVE_PROM:
            self.registry = registry or CollectorRegistry()
            r = self.registry
            q = ["stream_job", "query"]
            self.g_last = Gauge("waba_stream_last_progress_timestamp_seconds",
                                "Dernière progression (micro-lot ou idle)", q, registry=r)
            self.g_active = Gauge("waba_stream_query_active", "1 si la requête tourne", q,
                                  registry=r)
            self.g_in = Gauge("waba_stream_input_rows_per_second", "Débit d'entrée", q,
                              registry=r)
            self.g_proc = Gauge("waba_stream_processed_rows_per_second", "Débit traité", q,
                                registry=r)
            self.g_dur = Gauge("waba_stream_batch_duration_seconds", "Durée du dernier micro-lot",
                               q, registry=r)
            self.c_rows = Counter("waba_stream_input_rows", "Messages lus", q, registry=r)
            self.g_lag = Gauge("waba_stream_kafka_lag_messages",
                               "Messages en attente (dernier offset - offset traité)",
                               ["stream_job", "query", "topic"], registry=r)
            self.g_country = Gauge("waba_stream_country_lag_seconds",
                                   "Latence ingestion -> Silver du dernier micro-lot, par pays",
                                   ["stream_job", "country"], registry=r)
            self.c_dlq = Counter("waba_stream_dlq_messages", "Messages envoyés en DLQ",
                                 ["stream_job", "dataset"], registry=r)
            self.c_alerts = Counter("waba_stream_alerts", "Alertes émises", ["stream_job", "kind"],
                                    registry=r)
        else:
            self.registry = None
            self.g_last = self.g_active = self.g_in = self.g_proc = self.g_dur = _Noop()
            self.c_rows = self.g_lag = self.g_country = self.c_dlq = self.c_alerts = _Noop()

    # ------------------------------------------------------------------ événements
    def _name(self, qid: str) -> str:
        return self._names.get(qid, qid)

    def started(self, name: str, qid: str | None = None) -> None:
        with self._lock:
            if qid:
                self._names[qid] = name
            self.queries[name] = {"last": time.time(), "active": True}
        self.g_active.labels(self.job, name).set(1)
        self.g_last.labels(self.job, name).set(time.time())

    def progress(self, p: dict) -> None:
        name = p.get("name") or p.get("id", "unknown")
        now = time.time()
        with self._lock:
            self.queries.setdefault(name, {"active": True})["last"] = now
        self.g_last.labels(self.job, name).set(now)
        self.g_active.labels(self.job, name).set(1)
        self.g_in.labels(self.job, name).set(float(p.get("inputRowsPerSecond") or 0))
        self.g_proc.labels(self.job, name).set(float(p.get("processedRowsPerSecond") or 0))
        dur = (p.get("durationMs") or {}).get("triggerExecution")
        if dur is not None:
            self.g_dur.labels(self.job, name).set(float(dur) / 1000)
        rows = int(p.get("numInputRows") or 0)
        if rows:
            self.c_rows.labels(self.job, name).inc(rows)
        for src in p.get("sources") or []:
            for topic, lag in kafka_lag(src).items():
                self.g_lag.labels(self.job, name, topic).set(lag)

    def idle(self, qid: str) -> None:
        name = self._name(qid)
        now = time.time()
        with self._lock:
            self.queries.setdefault(name, {"active": True})["last"] = now
        self.g_last.labels(self.job, name).set(now)

    def terminated(self, qid: str, exception: str | None) -> None:
        name = self._name(qid)
        with self._lock:
            self.queries.setdefault(name, {})["active"] = False
            if exception:
                self.failed = f"{name}: {exception[:300]}"
        self.g_active.labels(self.job, name).set(0)

    def country_lag(self, lags: dict[str, float]) -> None:
        for cc, value in lags.items():
            self.g_country.labels(self.job, cc).set(value)

    def dlq(self, dataset: str, n: int) -> None:
        if n:
            self.c_dlq.labels(self.job, dataset).inc(n)

    def alerts(self, kind: str, n: int) -> None:
        if n:
            self.c_alerts.labels(self.job, kind).inc(n)

    # ------------------------------------------------------------------ sondes
    def status(self, max_age: float) -> tuple[bool, dict]:
        now = time.time()
        with self._lock:
            detail = {n: {"active": q.get("active", False),
                          "age_s": round(now - q.get("last", 0), 1)}
                      for n, q in self.queries.items()}
            failed = self.failed
        ok = (failed is None and bool(detail)
              and all(d["active"] and d["age_s"] < max_age for d in detail.values()))
        return ok, {"job": self.job, "queries": detail, "failed": failed}

    def listener(self):
        """StreamingQueryListener PySpark branché sur ces métriques."""
        from pyspark.sql.streaming import StreamingQueryListener

        metrics = self

        class _Listener(StreamingQueryListener):
            def onQueryStarted(self, event):
                metrics.started(event.name or str(event.id), str(event.id))

            def onQueryProgress(self, event):
                metrics.progress(json.loads(event.progress.json))

            def onQueryIdle(self, event):  # Spark >= 3.5
                metrics.idle(str(event.id))

            def onQueryTerminated(self, event):
                metrics.terminated(str(event.id), event.exception)

        return _Listener()

    def serve(self, port: int) -> ThreadingHTTPServer:
        metrics = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def _send(self, code: int, body: bytes, ctype: str) -> None:
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                if self.path.startswith("/metrics"):
                    if metrics.registry is None:
                        return self._send(200, b"", "text/plain")
                    return self._send(200, generate_latest(metrics.registry),
                                      CONTENT_TYPE_LATEST)
                if self.path.startswith("/healthz"):
                    ok, detail = metrics.status(LIVE_MAX_AGE_S)
                    # Pendant le démarrage (aucune requête encore enregistrée) : vivant.
                    ok = ok or (not detail["queries"] and detail["failed"] is None)
                elif self.path.startswith("/ready"):
                    ok, detail = metrics.status(READY_MAX_AGE_S)
                else:
                    return self._send(404, b"not found", "text/plain")
                return self._send(200 if ok else 503, json.dumps(detail).encode(),
                                  "application/json")

        srv = ThreadingHTTPServer(("0.0.0.0", port), Handler)
        threading.Thread(target=srv.serve_forever, daemon=True, name="metrics-http").start()
        return srv
