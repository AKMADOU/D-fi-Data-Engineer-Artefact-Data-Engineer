#!/usr/bin/env python3
"""Provisioning idempotent du flux NiFi « raw-landing (MinIO) -> Kafka » (Level 3).

Crée, dans un Process Group dédié, le flux suivant via l'API REST de NiFi 2.x :

  ListS3 (raw-landing, toutes les 5 s)
    -> RouteOnAttribute   : seulement <PAYS>/<flux événementiel>/*.csv déposés après le
                            provisioning (NIFI_REPLAY_HISTORY=true pour rejouer l'historique)
    -> FetchS3Object
    -> UpdateAttribute    : kafka.topic = raw-<flux> (ex. raw-bank-transactions)
    -> UpdateRecord       : CSV -> JSON (1 événement par ligne) + ingestion_timestamp,
                            source_file, landed_at
    -> PublishKafka       : topic ${kafka.topic}, 1 message par ligne
  UpdateRecord (échec de parsing) -> PublishKafka DLQ (dlq-financial-events)

Back-pressure : 10 000 flowfiles / 1 GB par connexion ; ListS3 cesse de lister tant que
la file aval est pleine, ce qui protège le broker Kafka d'une rafale de fichiers.

Aucune dépendance (bibliothèque standard). Tous les paramètres viennent de l'environnement.
Les noms de propriétés sont résolus par nom interne OU par libellé affiché, et les types
de composants par suffixe de classe : le script tolère les variations entre versions 2.x.
"""
from __future__ import annotations

import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

PG_NAME = "WABA - raw-landing -> Kafka"
EVENT_DATASETS = ("bank_transactions", "insurance_operations", "mobile_money_payments",
                  "loan_repayments")
TOPIC_EXPR = "${filename:getDelimitedField(2, '/'):replace('_', '-'):prepend('raw-')}"
ISO_NOW = "${now():format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", 'GMT')}"
ISO_LANDED = "${s3.lastModified:format(\"yyyy-MM-dd'T'HH:mm:ss.SSS'Z'\", 'GMT')}"
BACK_PRESSURE = {"backPressureObjectThreshold": 10000, "backPressureDataSizeThreshold": "1 GB"}


def log(**kw) -> None:
    print(json.dumps({"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                      "logger": "nifi-provision", **kw}), flush=True)


# --------------------------------------------------------------------------- helpers purs
def route_expression(since_ms: int | None) -> str:
    """Expression RouteOnAttribute : fichiers événementiels <CC>/<dataset>/*.csv [récents]."""
    # pas d'antislash : '[.]' plutôt que '\\.' (échappements du langage d'expression NiFi)
    regex = "^[A-Z]{2}/(" + "|".join(EVENT_DATASETS) + ")/[^/]+[.]csv$"
    match = "${filename:matches('" + regex + "')"
    if since_ms is None:
        return match + "}"
    return match + ":and(${s3.lastModified:ge(" + str(since_ms) + ")})}"


def topic_for(filename: str) -> str:
    """Équivalent Python de TOPIC_EXPR (sert aux tests)."""
    return "raw-" + filename.split("/")[1].replace("_", "-")


def resolve_type(types: list[dict], suffix: str) -> dict:
    """Trouve un type de composant par suffixe de classe (ex. '.ListS3')."""
    matches = [t for t in types if t["type"] == suffix.lstrip(".")
               or t["type"].endswith("." + suffix.lstrip("."))]
    if not matches:
        raise LookupError(f"Type NiFi introuvable : {suffix}")
    # Préférer la version de bundle la plus récente
    matches.sort(key=lambda t: t.get("bundle", {}).get("version", ""), reverse=True)
    return {"type": matches[0]["type"], "bundle": matches[0]["bundle"]}


def resolve_properties(descriptors: dict, wanted: dict, dynamic: bool = False) -> tuple[dict, list]:
    """Traduit {libellé ou nom: valeur} en {nom interne: valeur interne}.

    - la clé peut être le nom interne ou le displayName (insensible à la casse) ;
    - la valeur peut être la valeur interne ou le libellé d'une valeur autorisée ;
    - clé inconnue : propriété dynamique si `dynamic`, sinon ignorée (renvoyée à part)."""
    by_display = {d.get("displayName", n).lower(): n for n, d in descriptors.items()}
    out, skipped = {}, []
    for key, value in wanted.items():
        name = key if key in descriptors else by_display.get(key.lower())
        if name is None:
            if dynamic:
                out[key] = value
            else:
                skipped.append(key)
            continue
        allowed = descriptors[name].get("allowableValues") or []
        if value is not None and allowed:
            values = {a["allowableValue"]["value"] for a in allowed}
            if value not in values:
                labels = {a["allowableValue"]["displayName"].lower(): a["allowableValue"]["value"]
                          for a in allowed}
                value = labels.get(str(value).lower(), value)
        out[name] = value
    return out, skipped


# --------------------------------------------------------------------------- client REST
class Nifi:
    def __init__(self, base: str, username: str, password: str, verify_tls: bool = False):
        self.base = base.rstrip("/") + "/nifi-api"
        self.ctx = ssl.create_default_context()
        if not verify_tls:   # certificat auto-signé généré par l'image NiFi (réseau interne)
            self.ctx.check_hostname = False
            self.ctx.verify_mode = ssl.CERT_NONE
        self.username, self.password = username, password
        self.token: str | None = None

    def _call(self, method: str, path: str, body=None, form: dict | None = None, raw=False):
        data, headers = None, {}
        if form is not None:
            data = urllib.parse.urlencode(form).encode()
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        elif body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        req = urllib.request.Request(self.base + path, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, context=self.ctx, timeout=60) as resp:
                payload = resp.read().decode()
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")[:500]
            raise RuntimeError(f"{method} {path} -> HTTP {exc.code} : {detail}") from None
        if raw:
            return payload
        return json.loads(payload) if payload else {}

    def get(self, path):
        return self._call("GET", path)

    def post(self, path, body):
        return self._call("POST", path, body)

    def put(self, path, body):
        return self._call("PUT", path, body)

    def delete(self, path):
        return self._call("DELETE", path)

    def login(self, timeout_s: int = 600) -> None:
        deadline = time.time() + timeout_s
        while True:
            try:
                self.token = self._call("POST", "/access/token", raw=True,
                                        form={"username": self.username,
                                              "password": self.password})
                return
            except Exception as exc:  # NiFi démarre en 1-3 min
                if time.time() > deadline:
                    raise RuntimeError(f"NiFi injoignable : {exc}") from exc
                log(status="WAITING", reason=str(exc)[:160])
                time.sleep(10)

    # ------------------------------------------------------------------ composants
    def create_service(self, pg: str, ctype: dict, name: str, props: dict) -> str:
        ent = self.post(f"/process-groups/{pg}/controller-services",
                        {"revision": {"version": 0},
                         "component": {**ctype, "name": name}})
        sid = ent["id"]
        resolved, skipped = resolve_properties(ent["component"]["descriptors"], props)
        if skipped:
            log(warning="propriétés ignorées", component=name, skipped=skipped)
        ent = self.put(f"/controller-services/{sid}",
                       {"revision": ent["revision"],
                        "component": {"id": sid, "properties": resolved}})
        return sid

    def enable_services(self, pg: str) -> None:
        self.put(f"/flow/process-groups/{pg}/controller-services",
                 {"id": pg, "state": "ENABLED"})

    def create_processor(self, pg: str, ptype: dict, name: str, pos: tuple[int, int],
                         props: dict, dynamic: bool = False, schedule: str | None = None,
                         terminate: list[str] | None = None) -> str:
        ent = self.post(f"/process-groups/{pg}/processors",
                        {"revision": {"version": 0},
                         "component": {**ptype, "name": name,
                                       "position": {"x": pos[0], "y": pos[1]}}})
        pid = ent["id"]
        resolved, skipped = resolve_properties(ent["component"]["config"]["descriptors"],
                                               props, dynamic)
        if skipped:
            log(warning="propriétés ignorées", component=name, skipped=skipped)
        config = {"properties": resolved, "autoTerminatedRelationships": terminate or []}
        if schedule:
            config["schedulingPeriod"] = schedule
        self.put(f"/processors/{pid}", {"revision": ent["revision"],
                                        "component": {"id": pid, "config": config}})
        return pid

    def connect(self, pg: str, src: str, dst: str, rels: list[str]) -> str:
        ent = self.post(f"/process-groups/{pg}/connections", {
            "revision": {"version": 0},
            "component": {"source": {"id": src, "groupId": pg, "type": "PROCESSOR"},
                          "destination": {"id": dst, "groupId": pg, "type": "PROCESSOR"},
                          "selectedRelationships": rels, **BACK_PRESSURE}})
        return ent["id"]

    def find_group(self, parent: str, name: str) -> dict | None:
        groups = self.get(f"/process-groups/{parent}/process-groups")["processGroups"]
        return next((g for g in groups if g["component"]["name"] == name), None)

    def drop_group(self, group: dict) -> None:
        gid = group["id"]
        self.put(f"/flow/process-groups/{gid}", {"id": gid, "state": "STOPPED"})
        self.put(f"/flow/process-groups/{gid}/controller-services",
                 {"id": gid, "state": "DISABLED"})
        for conn in self.get(f"/process-groups/{gid}/connections")["connections"]:
            self._call("POST", f"/flowfile-queues/{conn['id']}/drop-requests")
        time.sleep(5)
        fresh = self.get(f"/process-groups/{gid}")
        self.delete(f"/process-groups/{gid}?version={fresh['revision']['version']}")


# --------------------------------------------------------------------------- flux
def build_flow(nifi: Nifi, env: dict) -> str:
    root = nifi.get("/flow/process-groups/root")["processGroupFlow"]["id"]
    existing = nifi.find_group(root, PG_NAME)
    if existing and env.get("NIFI_RECREATE", "false").lower() != "true":
        log(status="SKIPPED", reason="flux déjà provisionné (NIFI_RECREATE=true pour le recréer)",
            process_group=existing["id"])
        nifi.put(f"/flow/process-groups/{existing['id']}",
                 {"id": existing["id"], "state": "RUNNING"})
        return existing["id"]
    if existing:
        nifi.drop_group(existing)
        log(status="DELETED", process_group=existing["id"])

    pg = nifi.post(f"/process-groups/{root}/process-groups",
                   {"revision": {"version": 0},
                    "component": {"name": PG_NAME, "position": {"x": 0, "y": 0}}})["id"]
    ptypes = nifi.get("/flow/processor-types")["processorTypes"]
    stypes = nifi.get("/flow/controller-service-types")["controllerServiceTypes"]
    P = lambda s: resolve_type(ptypes, s)  # noqa: E731
    C = lambda s: resolve_type(stypes, s)  # noqa: E731

    creds = nifi.create_service(pg, C("AWSCredentialsProviderControllerService"),
                                "MinIO credentials",
                                {"Access Key ID": env["S3_ACCESS_KEY"],
                                 "Secret Access Key": env["S3_SECRET_KEY"]})
    csv = nifi.create_service(pg, C("CSVReader"), "CSV (en-tête, champs texte)",
                              {"Schema Access Strategy": "Use String Fields From Header",
                               "Treat First Line as Header": "true",
                               "Trim Fields": "true"})
    json_w = nifi.create_service(pg, C("JsonRecordSetWriter"), "JSON writer",
                                 {"Output Grouping": "One Line Per Object",
                                  "Suppress Null Values": "Never Suppress"})
    json_r = nifi.create_service(pg, C("JsonTreeReader"), "JSON reader", {})
    kafka = nifi.create_service(pg, C("Kafka3ConnectionService"), "Kafka",
                                {"Bootstrap Servers": env["KAFKA_BOOTSTRAP_SERVERS"]})
    nifi.enable_services(pg)

    s3 = {"AWS Credentials Provider Service": creds, "Region": env.get("AWS_REGION", "us-east-1"),
          "Endpoint Override URL": env["S3_ENDPOINT"], "Use Path Style Access": "true"}
    since = None if env.get("NIFI_REPLAY_HISTORY", "false").lower() == "true" \
        else int(time.time() * 1000)

    list_s3 = nifi.create_processor(pg, P("ListS3"), "ListS3 raw-landing", (0, 0),
                                    {**s3, "Bucket": env.get("LANDING_BUCKET", "raw-landing")},
                                    schedule=env.get("NIFI_LIST_INTERVAL", "5 sec"))
    route = nifi.create_processor(pg, P("RouteOnAttribute"), "Fichiers événementiels", (0, 200),
                                  {"Routing Strategy": "Route to Property name",
                                   "events": route_expression(since)},
                                  dynamic=True, terminate=["unmatched"])
    fetch = nifi.create_processor(pg, P("FetchS3Object"), "FetchS3Object", (0, 400),
                                  {**s3, "Bucket": "${s3.bucket}", "Object Key": "${filename}"})
    topic = nifi.create_processor(pg, P("UpdateAttribute"), "Topic selon le flux", (0, 600),
                                  {"kafka.topic": TOPIC_EXPR,
                                   "waba.country": "${filename:getDelimitedField(1, '/')}"},
                                  dynamic=True)
    enrich = nifi.create_processor(pg, P("UpdateRecord"), "CSV -> JSON + métadonnées", (0, 800),
                                   {"Record Reader": csv, "Record Writer": json_w,
                                    "Replacement Value Strategy": "Literal Value",
                                    "/ingestion_timestamp": ISO_NOW,
                                    "/source_file": "s3://${s3.bucket}/${filename}",
                                    "/landed_at": ISO_LANDED},
                                   dynamic=True)
    publish = nifi.create_processor(pg, P("PublishKafka"), "PublishKafka raw-*", (0, 1000),
                                    {"Kafka Connection Service": kafka,
                                     "Topic Name": "${kafka.topic}",
                                     "Record Reader": json_r, "Record Writer": json_w,
                                     "Transactions Enabled": "false",
                                     "Delivery Guarantee": "Guarantee Replicated Delivery"},
                                    terminate=["success"])
    dlq = nifi.create_processor(pg, P("PublishKafka"), "PublishKafka DLQ", (500, 1000),
                                {"Kafka Connection Service": kafka,
                                 "Topic Name": "dlq-financial-events",
                                 "Transactions Enabled": "false"},
                                terminate=["success"])

    nifi.connect(pg, list_s3, route, ["success"])
    nifi.connect(pg, route, fetch, ["events"])
    nifi.connect(pg, fetch, topic, ["success"])
    nifi.connect(pg, fetch, fetch, ["failure"])            # nouvel essai (pénalité)
    nifi.connect(pg, topic, enrich, ["success"])
    nifi.connect(pg, enrich, publish, ["success"])
    nifi.connect(pg, enrich, dlq, ["failure"])              # fichier illisible -> DLQ
    nifi.connect(pg, publish, publish, ["failure"])         # Kafka indisponible : on réessaie
    nifi.connect(pg, dlq, dlq, ["failure"])

    nifi.put(f"/flow/process-groups/{pg}", {"id": pg, "state": "RUNNING"})
    log(status="CREATED", process_group=pg, replay_history=since is None,
        topics=[topic_for(f"CI/{d}/x.csv") for d in EVENT_DATASETS])
    return pg


def main() -> int:
    env = dict(os.environ)
    missing = [k for k in ("NIFI_URL", "NIFI_USERNAME", "NIFI_PASSWORD", "S3_ENDPOINT",
                           "S3_ACCESS_KEY", "S3_SECRET_KEY", "KAFKA_BOOTSTRAP_SERVERS")
               if not env.get(k)]
    if missing:
        log(status="ERROR", missing=missing)
        return 2
    nifi = Nifi(env["NIFI_URL"], env["NIFI_USERNAME"], env["NIFI_PASSWORD"],
                verify_tls=env.get("NIFI_VERIFY_TLS", "false").lower() == "true")
    nifi.login()
    build_flow(nifi, env)
    return 0


if __name__ == "__main__":
    sys.exit(main())
