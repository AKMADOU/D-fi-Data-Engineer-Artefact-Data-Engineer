"""Récepteur webhook des alertes Grafana (démo) : journalise chaque notification en JSON
(visible dans `kubectl logs` et dans Loki) et garde les 200 dernières, consultables
sur GET /alerts. Remplaçable par Slack / Teams / PagerDuty en production."""
import json
import time
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

RECEIVED = deque(maxlen=200)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _reply(self, code, body):
        data = json.dumps(body, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers.get("Content-Length") or 0)) or b"{}")
        for alert in payload.get("alerts", []):
            entry = {"received_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                     "status": alert.get("status"),
                     "alertname": alert.get("labels", {}).get("alertname"),
                     "severity": alert.get("labels", {}).get("severity"),
                     "summary": alert.get("annotations", {}).get("summary"),
                     "startsAt": alert.get("startsAt")}
            RECEIVED.append(entry)
            print(json.dumps({"level": "WARNING", "logger": "alert-webhook", "msg": entry},
                             ensure_ascii=False), flush=True)
        self._reply(200, {"ok": True})

    def do_GET(self):
        if self.path.startswith("/healthz"):
            return self._reply(200, {"ok": True})
        self._reply(200, list(RECEIVED))


ThreadingHTTPServer(("0.0.0.0", 8080), Handler).serve_forever()
