"""Tests du provisioning NiFi contre un faux serveur REST (aucun NiFi requis).

    cd nifi && python -m pytest -q tests
"""
from __future__ import annotations

import json
import re
import sys
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import provision as P  # noqa: E402

DESCRIPTORS = {
    # nom interne -> (libellé, valeurs autorisées [(valeur, libellé)])
    "schema-access-strategy": ("Schema Access Strategy",
                               [("csv-header-derived", "Use String Fields From Header"),
                                ("infer-schema", "Infer Schema")]),
    "Bucket": ("Bucket", []),
    "Topic Name": ("Topic Name", []),
    "replacement-value-strategy": ("Replacement Value Strategy",
                                   [("literal-value", "Literal Value"),
                                    ("record-path-value", "Record Path Value")]),
    "record-reader": ("Record Reader", []),
    "Region": ("Region", [("us-east-1", "US East (N. Virginia)")]),
}


def descriptors():
    return {n: {"name": n, "displayName": d,
                "allowableValues": [{"allowableValue": {"value": v, "displayName": l}}
                                    for v, l in vals]}
            for n, (d, vals) in DESCRIPTORS.items()}


class FakeNifi:
    def __init__(self):
        self.calls, self.components, self.n = [], {}, 0

    def new_id(self):
        self.n += 1
        return f"id-{self.n}"

    def handle(self, method, path, body):
        self.calls.append((method, path, body))
        if path == "/nifi-api/access/token":
            return "jwt-token"
        if path == "/nifi-api/flow/process-groups/root":
            return {"processGroupFlow": {"id": "root"}}
        if path.endswith("/process-groups") and method == "GET":
            return {"processGroups": [c for c in self.components.values()
                                      if c.get("kind") == "pg"]}
        if path == "/nifi-api/flow/processor-types":
            return {"processorTypes": [
                {"type": f"org.apache.nifi.{x}", "bundle": {"version": "2.12.0"}}
                for x in ("processors.aws.s3.ListS3", "processors.aws.s3.FetchS3Object",
                          "processors.standard.RouteOnAttribute",
                          "processors.attributes.UpdateAttribute",
                          "processors.standard.UpdateRecord", "kafka.processors.PublishKafka")]}
        if path == "/nifi-api/flow/controller-service-types":
            return {"controllerServiceTypes": [
                {"type": f"org.apache.nifi.{x}", "bundle": {"version": "2.12.0"}}
                for x in ("processors.aws.credentials.provider.service."
                          "AWSCredentialsProviderControllerService", "csv.CSVReader",
                          "json.JsonRecordSetWriter", "json.JsonTreeReader",
                          "kafka.service.Kafka3ConnectionService")]}
        if method == "POST" and re.search(r"/(process-groups|controller-services|processors|"
                                          r"connections)$", path):
            cid = self.new_id()
            kind = path.rsplit("/", 1)[1]
            comp = {"id": cid, "kind": "pg" if kind == "process-groups" else kind,
                    "component": {**body["component"], "descriptors": descriptors(),
                                  "config": {"descriptors": descriptors()}},
                    "revision": {"version": 1}}
            self.components[cid] = comp
            return comp
        if method == "PUT":
            return {"ok": True}
        return {}


@pytest.fixture()
def fake():
    state = FakeNifi()

    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def _do(self, method):
            n = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(n).decode() if n else ""
            body = json.loads(raw) if raw and raw.startswith("{") else raw
            out = state.handle(method, self.path, body)
            data = out.encode() if isinstance(out, str) else json.dumps(out).encode()
            self.send_response(200)
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            self._do("GET")

        def do_POST(self):
            self._do("POST")

        def do_PUT(self):
            self._do("PUT")

    srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield state, f"http://127.0.0.1:{srv.server_port}"
    srv.shutdown()


def test_helpers():
    assert P.topic_for("CI/mobile_money_payments/mm_pay_CI_20260101_01.csv") \
        == "raw-mobile-money-payments"
    expr = P.route_expression(123)
    assert "\\" not in expr and expr.endswith(":and(${s3.lastModified:ge(123)})}")
    assert re.match(expr.split("'")[1], "GH/loan_repayments/loan_rep_GH_20260101_01.csv")
    assert not re.match(expr.split("'")[1], "referentials/customers/customers_full_01.csv")
    props, skipped = P.resolve_properties(
        descriptors(), {"Schema Access Strategy": "Use String Fields From Header",
                        "Region": "us-east-1", "Unknown": "x"})
    assert props == {"schema-access-strategy": "csv-header-derived", "Region": "us-east-1"}
    assert skipped == ["Unknown"]
    dyn, _ = P.resolve_properties(descriptors(), {"/source_file": "s3://x"}, dynamic=True)
    assert dyn == {"/source_file": "s3://x"}


def test_build_flow_against_fake_api(fake):
    state, url = fake
    nifi = P.Nifi(url, "admin", "secret-password")
    nifi.login(timeout_s=5)
    env = {"S3_ACCESS_KEY": "k", "S3_SECRET_KEY": "s", "S3_ENDPOINT": "http://minio:9000",
           "KAFKA_BOOTSTRAP_SERVERS": "kafka:9092"}
    pg = P.build_flow(nifi, env)
    posts = [(p, b) for m, p, b in state.calls if m == "POST"]
    processors = [b["component"]["name"] for p, b in posts if p.endswith("/processors")]
    assert len(processors) == 7
    connections = [b["component"] for p, b in posts if p.endswith("/connections")]
    assert len(connections) == 9
    assert all(c["backPressureObjectThreshold"] == 10000 for c in connections)
    # topic dynamique et métadonnées d'enrichissement envoyés à UpdateAttribute / UpdateRecord
    puts = json.dumps([b for m, p, b in state.calls if m == "PUT"])
    assert "raw-" in puts and "/ingestion_timestamp" in puts and "/source_file" in puts
    assert "csv-header-derived" in puts and "literal-value" in puts
    # démarrage du groupe
    assert any(m == "PUT" and p == f"/nifi-api/flow/process-groups/{pg}"
               and b.get("state") == "RUNNING" for m, p, b in state.calls)
    # authentification Bearer utilisée
    assert state.calls[0][1] == "/nifi-api/access/token"
