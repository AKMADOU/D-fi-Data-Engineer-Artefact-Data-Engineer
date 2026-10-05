"""Tests du script de gouvernance contre un faux serveur OpenMetadata (sans OpenMetadata).

    python -m pytest -q governance/tests
"""
from __future__ import annotations

import json
import sys
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import unquote

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class FakeOM:
    def __init__(self):
        self.calls, self.entities, self.lineage, self.patches = [], {}, [], {}
        cols = {"bronze": ["transaction_id", "account_id", "customer_id", "amount"],
                "silver": ["transaction_id", "account_key", "customer_key", "amount_eur"],
                "gold": ["country_code", "subject_key", "amount_eur"]}
        import openmetadata_bootstrap as B
        for schema, names in (("bronze", B.EVENTS + B.REFERENTIALS),
                              ("silver", B.EVENTS + B.REFERENTIALS
                               + [f"rt_{d}" for d in B.EVENTS]),
                              ("gold", list(B.GOLD_DOCS))):
            for n in names:
                self.add("tables", f"{B.TRINO_SERVICE}.{B.CATALOG}.{schema}.{n}",
                         columns=[{"name": c, "tags": []} for c in cols[schema]], tags=[])
        for t in list(B.RAW_TOPICS.values()) + list(B.SILVER_TOPICS.values()):
            self.add("topics", f"{B.KAFKA_SERVICE}.{t}")
        self.add("bots", "ingestion-bot", botUser={"id": "bot-user"})

    def add(self, kind, fqn, **kw):
        self.entities[(kind, fqn)] = {"id": str(uuid.uuid4()), "fullyQualifiedName": fqn, **kw}

    def handle(self, method, path, body):
        self.calls.append((method, path))
        p = path.split("?")[0].removeprefix("/api/v1")
        if p == "/system/version":
            return 200, {"version": "1.13.6"}
        if p == "/users/login":
            return 200, {"accessToken": "admin-jwt"}
        if p.startswith("/users/auth-mechanism/"):
            return 200, {"config": {"JWTToken": "bot-jwt"}}
        if "/name/" in p:
            kind, fqn = p.strip("/").split("/name/")
            ent = self.entities.get((kind, unquote(fqn)))
            return (200, ent) if ent else (404, {"message": "not found"})
        if method == "PUT" and p == "/lineage":
            self.lineage.append(body["edge"])
            return 200, {}
        if method == "PUT":
            return 200, {"id": str(uuid.uuid5(uuid.NAMESPACE_URL, p + body["name"])),
                         **body}
        if method == "PATCH":
            self.patches.setdefault(p, []).extend(body)
            return 200, {}
        return 404, {}


@pytest.fixture()
def om_server(monkeypatch):
    import openmetadata_bootstrap as B
    state = FakeOM()

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _do(self, method):
            n = int(self.headers.get("Content-Length") or 0)
            body = json.loads(self.rfile.read(n)) if n else None
            code, out = state.handle(method, self.path, body)
            data = json.dumps(out).encode()
            self.send_response(code)
            self.end_headers()
            self.wfile.write(data)

        do_GET = lambda self: self._do("GET")      # noqa: E731
        do_PUT = lambda self: self._do("PUT")      # noqa: E731
        do_POST = lambda self: self._do("POST")    # noqa: E731
        do_PATCH = lambda self: self._do("PATCH")  # noqa: E731

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{srv.server_port}"
    monkeypatch.setattr(B, "OM_URL", base)
    monkeypatch.setattr(B, "API", f"{base}/api/v1")
    yield state
    srv.shutdown()


def test_prepare_writes_workflows(om_server, tmp_path, monkeypatch):
    import openmetadata_bootstrap as B
    monkeypatch.setattr(B, "WORK", tmp_path)
    monkeypatch.setenv("TRINO_OPENMETADATA_PASSWORD", "pw")
    assert B.prepare() == 0
    trino = json.loads((tmp_path / "trino.yaml").read_text())
    cfg = trino["source"]["serviceConnection"]["config"]
    assert cfg["authType"] == {"password": "pw"} and cfg["connectionArguments"]["verify"] is False
    assert trino["workflowConfig"]["openMetadataServerConfig"]["securityConfig"]["jwtToken"] == "bot-jwt"
    assert (tmp_path / "kafka.yaml").exists()


def test_govern_documents_tags_and_lineage(om_server):
    import openmetadata_bootstrap as B
    assert B.govern() == 0
    gold_patches = {p: ops for p, ops in om_server.patches.items()
                    if any(o["path"] == "/owners" for o in ops)}
    assert len(gold_patches) >= 5                        # >= 5 tables Gold documentées
    ops = next(iter(gold_patches.values()))
    tags = next(o["value"] for o in ops if o["path"] == "/tags")
    assert any(t["tagFQN"].startswith("Reglementaire.") or t["source"] == "Glossary"
               or t["tagFQN"] == "Tier.Tier1" for t in tags)
    pii = [o for ops in om_server.patches.values() for o in ops if o["path"].startswith("/columns/")]
    assert any(t["tagFQN"] == "PII.Sensitive" for o in pii for t in o["value"])
    kinds = {(e["fromEntity"]["type"], e["toEntity"]["type"]) for e in om_server.lineage}
    assert {("container", "table"), ("table", "table"), ("container", "topic"),
            ("topic", "table")} <= kinds
    # raw -> bronze -> silver -> gold : chaque maillon est présent
    assert len(om_server.lineage) > 40
