#!/usr/bin/env python3
"""Gouvernance OpenMetadata du lakehouse WABA (Level 4) — idempotent, bibliothèque standard.

Deux sous-commandes, exécutées par le Job Kubernetes `openmetadata-governance` :

  prepare   attend le serveur, récupère le jeton du bot d'ingestion et écrit les
            workflows d'ingestion Trino (iceberg : bronze/silver/gold) et Kafka
            dans WORK_DIR ; le conteneur suivant lance `metadata ingest -c ...`.
  govern    après l'ingestion :
            * classification « Reglementaire » (BCEAO, CIMA, AML, RGPD) ;
            * équipes propriétaires (entités WABA) ;
            * glossaire financier (NPL, loss ratio, ARPC, ...) ;
            * documentation des tables Gold (description métier, propriétaire,
              tags réglementaires, termes du glossaire) ;
            * tags PII.Sensitive sur les identifiants clients / comptes (Bronze) et
              leurs versions pseudonymisées (Silver) ;
            * lineage raw (MinIO raw-landing) -> Bronze -> Silver -> Gold, et la
              branche temps réel (topics Kafka -> silver.rt_* -> alertes Gold).
"""
from __future__ import annotations

import base64
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

OM_URL = os.environ.get("OM_URL", "http://openmetadata.governance.svc.cluster.local:8585").rstrip("/")
API = f"{OM_URL}/api/v1"
WORK = Path(os.environ.get("WORK_DIR", "/work"))
TRINO_SERVICE = "waba_trino"
KAFKA_SERVICE = "waba_kafka"
STORAGE_SERVICE = "waba_minio"
CATALOG = "iceberg"
GLOSSARY = "Glossaire_Financier_WABA"
CLASSIF = "Reglementaire"

EVENTS = ["bank_transactions", "insurance_operations", "mobile_money_payments",
          "loan_repayments"]
REFERENTIALS = ["customers", "accounts", "branches", "products"]
RAW_TOPICS = {d: "raw-" + d.replace("_", "-") for d in EVENTS}
SILVER_TOPICS = {"bank_transactions": "silver-bank-transactions",
                 "insurance_operations": "silver-insurance-operations",
                 "mobile_money_payments": "silver-mobile-money",
                 "loan_repayments": "silver-loan-repayments"}

TEAMS = {
    "waba-banque": "WABA Banque",
    "waba-assurance": "WABA Assurance",
    "waba-mobile-money": "WABA Mobile Money",
    "waba-conformite": "Direction Conformité Groupe",
    "waba-data-platform": "Plateforme Data Groupe",
}
REG_TAGS = {
    "BCEAO": "Soumis à la réglementation prudentielle BCEAO (UEMOA) : reporting, seuils NPL, réserves",
    "CIMA": "Soumis au Code CIMA (assurance, zone CIMA) : sinistralité, provisions",
    "AML": "Lutte contre le blanchiment (LBC/FT) : déclarations d'opérations au-delà des seuils",
    "RGPD": "Données personnelles : RGPD et lois locales (ex. loi ivoirienne 2013-450, loi sénégalaise 2008-12)",
}
GLOSSARY_TERMS = {
    "NPL": "Non-Performing Loan : créance douteuse (défaut ou retard > 90 jours). "
           "Taux NPL = encours douteux / encours total ; seuil d'alerte BCEAO 5 %.",
    "Loss_Ratio": "Sinistralité : sinistres payés / primes acquises. Seuil de vigilance CIMA 70 %.",
    "ARPC": "Average Revenue Per Customer : revenus (commissions + intérêts) / clients actifs du mois.",
    "Encours": "Montant restant dû sur les prêts (valeur positive du solde débiteur), en EUR.",
    "AML": "Anti-Money Laundering : virement au-delà du seuil déclaratif "
           "(1 000 000 XOF en UEMOA, 5 000 GHS au Ghana).",
    "Corridor": "Couple pays émetteur -> pays bénéficiaire d'un transfert transfrontalier.",
    "Reserve_Liquidite": "Réserves obligatoires BCEAO : 3 % des dépôts ; alerte si les sorties "
                         "nettes sur 5 min dépassent 50 % de la réserve.",
    "Pseudonymisation": "Remplacement d'un identifiant par un hachage SHA-256 salé (clé *_key) : "
                        "reste une donnée personnelle au sens du RGPD.",
}
GOLD_DOCS = {
    # table : (description, équipe, tags réglementaires, termes du glossaire)
    "daily_transaction_volume": (
        "Volume et montant des transactions (banque + mobile money) par jour, pays, entité et "
        "type, en devise locale et en EUR. Base de la requête Lambda (batch + Kafka).",
        "waba-banque", ["BCEAO"], []),
    "npl_ratio_by_country": (
        "Taux de créances douteuses par pays et type de prêt, avec seuil BCEAO de 5 % et "
        "indicateur de dépassement.", "waba-banque", ["BCEAO"], ["NPL", "Encours"]),
    "customer_arpu_monthly": (
        "Revenu moyen par client actif (ARPC), par mois, pays et segment client.",
        "waba-banque", [], ["ARPC"]),
    "loss_ratio_by_product": (
        "Sinistres payés / primes acquises par mois, pays et produit d'assurance ; seuil CIMA 70 %.",
        "waba-assurance", ["CIMA"], ["Loss_Ratio"]),
    "claims_processing_time": (
        "Délai de traitement des sinistres (moyenne, médiane, P90) par pays et famille IARD / VIE.",
        "waba-assurance", ["CIMA"], []),
    "mobile_money_daily_flow": (
        "Flux journaliers de paiements mobile money par pays : volumes, échecs, frais, "
        "utilisateurs actifs.", "waba-mobile-money", ["BCEAO"], []),
    "cross_border_transfers": (
        "Transferts transfrontaliers hebdomadaires par corridor et canal (virement, mobile money).",
        "waba-mobile-money", ["BCEAO", "AML"], ["Corridor"]),
    "regulatory_bceao_daily": (
        "Déclaration quotidienne J+1 à la BCEAO par pays : transactions, opérations importantes, "
        "sorties transfrontalières, dépôts, encours.", "waba-conformite", ["BCEAO"],
        ["Encours"]),
    "regulatory_cima_daily": (
        "Déclaration quotidienne J+1 CIMA par pays : primes et sinistres du mois, sinistres en "
        "attente, délai moyen.", "waba-conformite", ["CIMA"], ["Loss_Ratio"]),
    "aml_events": (
        "Événements AML temps réel (Level 3) : virements au-delà du seuil déclaratif.",
        "waba-conformite", ["AML"], ["AML"]),
    "fraud_alerts": (
        "Alertes de fraude temps réel : rafales de grosses transactions, pays inhabituel, "
        "sinistre > 3 x primes.", "waba-conformite", ["AML"], []),
    "liquidity_alerts": (
        "Alertes de liquidité : sorties nettes par pays au-delà de 50 % de la réserve BCEAO.",
        "waba-conformite", ["BCEAO"], ["Reserve_Liquidite"]),
}
GOLD_SOURCES = {
    "daily_transaction_volume": ["bank_transactions", "mobile_money_payments"],
    "npl_ratio_by_country": ["loan_repayments", "accounts"],
    "customer_arpu_monthly": ["bank_transactions", "mobile_money_payments", "loan_repayments",
                              "customers"],
    "loss_ratio_by_product": ["insurance_operations"],
    "claims_processing_time": ["insurance_operations"],
    "mobile_money_daily_flow": ["mobile_money_payments"],
    "cross_border_transfers": ["bank_transactions", "mobile_money_payments"],
    "regulatory_bceao_daily": ["bank_transactions", "mobile_money_payments", "accounts"],
    "regulatory_cima_daily": ["insurance_operations"],
}
SILVER_SOURCES = {
    "customers": ["customers"], "accounts": ["accounts", "customers", "products"],
    "branches": ["branches"], "products": ["products"],
    "bank_transactions": ["bank_transactions", "accounts", "customers", "branches"],
    "insurance_operations": ["insurance_operations", "accounts", "customers"],
    "mobile_money_payments": ["mobile_money_payments", "customers"],
    "loan_repayments": ["loan_repayments", "accounts", "customers", "products"],
}
STREAM_ALERTS = {"fraud_alerts": ["bank_transactions", "mobile_money_payments",
                                  "insurance_operations"],
                 "aml_events": ["bank_transactions", "mobile_money_payments"],
                 "liquidity_alerts": ["bank_transactions"]}
# Identifiants directs (Bronze) et pseudonymisés (Silver / Gold)
PII_BRONZE = {"customer_id", "account_id", "beneficiary_account", "sender_id", "receiver_id",
              "loan_account_id"}
PII_SILVER = {"customer_key", "account_key", "beneficiary_account_key", "sender_key",
              "receiver_key", "subject_key"}


def log(**kw) -> None:
    print(json.dumps({"logger": "openmetadata-governance", **kw}, ensure_ascii=False), flush=True)


# --------------------------------------------------------------------------- client
class OM:
    def __init__(self, token: str | None = None):
        self.token = token

    def call(self, method: str, path: str, body=None, patch: bool = False, ok404=False):
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Content-Type": "application/json-patch+json" if patch
                   else "application/json"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        req = urllib.request.Request(API + path, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=60) as r:
                raw = r.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as exc:
            if exc.code == 404 and ok404:
                return None
            raise RuntimeError(f"{method} {path} -> {exc.code} "
                               f"{exc.read().decode(errors='replace')[:300]}") from None

    def get(self, path, ok404=False):
        return self.call("GET", path, ok404=ok404)

    def put(self, path, body):
        return self.call("PUT", path, body)

    def patch(self, path, ops):
        return self.call("PATCH", path, ops, patch=True) if ops else None

    def by_name(self, kind: str, fqn: str, fields: str = "") -> dict | None:
        q = f"?fields={fields}" if fields else ""
        return self.get(f"/{kind}/name/{urllib.parse.quote(fqn, safe='')}{q}", ok404=True)


def wait_ready(timeout_s: int = 900) -> None:
    deadline = time.time() + timeout_s
    while True:
        try:
            with urllib.request.urlopen(f"{API}/system/version", timeout=10) as r:
                log(status="READY", version=json.loads(r.read()).get("version"))
                return
        except Exception as exc:
            if time.time() > deadline:
                raise SystemExit(f"OpenMetadata injoignable : {exc}")
            log(status="WAITING", reason=str(exc)[:120])
            time.sleep(10)


def admin_login() -> OM:
    """Compte admin par défaut d'OpenMetadata (authentification basique) ; si
    OM_ADMIN_PASSWORD est défini et que le mot de passe a été changé, on l'utilise."""
    email = os.environ.get("OM_ADMIN_EMAIL", "admin@open-metadata.org")
    for pwd in filter(None, [os.environ.get("OM_ADMIN_PASSWORD"), "admin"]):
        try:
            res = OM().call("POST", "/users/login",
                            {"email": email, "password": base64.b64encode(pwd.encode()).decode()})
            return OM(res["accessToken"])
        except RuntimeError:
            continue
    raise SystemExit("connexion admin OpenMetadata impossible")


# --------------------------------------------------------------------------- prepare
def ingestion_workflows(bot_token: str, env: dict) -> dict[str, dict]:
    server = {"hostPort": f"{OM_URL}/api", "authProvider": "openmetadata",
              "securityConfig": {"jwtToken": bot_token}}
    trino = {
        "source": {
            "type": "trino", "serviceName": TRINO_SERVICE,
            "serviceConnection": {"config": {
                "type": "Trino", "scheme": "trino",
                "hostPort": env.get("TRINO_HOSTPORT", "trino.serving.svc.cluster.local:8443"),
                "username": "openmetadata",
                "authType": {"password": env["TRINO_OPENMETADATA_PASSWORD"]},
                "catalog": CATALOG,
                # HTTPS interne, certificat auto-signé du cluster
                "connectionArguments": {"http_scheme": "https", "verify": False}}},
            "sourceConfig": {"config": {
                "type": "DatabaseMetadata", "markDeletedTables": True, "includeViews": True,
                "schemaFilterPattern": {"includes": ["^bronze$", "^silver$", "^gold$"]}}}},
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {"loggerLevel": "INFO", "openMetadataServerConfig": server},
    }
    kafka = {
        "source": {
            "type": "kafka", "serviceName": KAFKA_SERVICE,
            "serviceConnection": {"config": {
                "type": "Kafka",
                "bootstrapServers": env.get("KAFKA_BOOTSTRAP_SERVERS",
                                            "kafka.ingestion.svc.cluster.local:9092")}},
            "sourceConfig": {"config": {"type": "MessagingMetadata",
                                        "topicFilterPattern": {"includes": ["^(raw|silver|gold|dlq)-.*"]}}}},
        "sink": {"type": "metadata-rest", "config": {}},
        "workflowConfig": {"loggerLevel": "INFO", "openMetadataServerConfig": server},
    }
    return {"trino.yaml": trino, "kafka.yaml": kafka}


def prepare() -> int:
    wait_ready()
    om = admin_login()
    bot = om.by_name("bots", "ingestion-bot")
    user_id = bot["botUser"]["id"]
    token = om.get(f"/users/auth-mechanism/{user_id}")["config"]["JWTToken"]
    WORK.mkdir(parents=True, exist_ok=True)
    for name, wf in ingestion_workflows(token, dict(os.environ)).items():
        # JSON est un sous-ensemble de YAML : `metadata ingest -c` le lit tel quel
        (WORK / name).write_text(json.dumps(wf, indent=2))
    log(status="PREPARED", files=sorted(p.name for p in WORK.iterdir()))
    return 0


# --------------------------------------------------------------------------- govern
def table_fqn(schema: str, table: str) -> str:
    return f"{TRINO_SERVICE}.{CATALOG}.{schema}.{table}"


def tag_label(fqn: str, source: str = "Classification") -> dict:
    return {"tagFQN": fqn, "source": source, "labelType": "Manual", "state": "Confirmed"}


def ensure_taxonomy(om: OM) -> dict[str, str]:
    om.put("/classifications", {"name": CLASSIF, "displayName": "Réglementaire",
                                "description": "Périmètres réglementaires des données WABA"})
    for tag, desc in REG_TAGS.items():
        om.put("/tags", {"name": tag, "classification": CLASSIF, "description": desc})
    teams = {}
    for name, display in TEAMS.items():
        teams[name] = om.put("/teams", {"name": name, "displayName": display,
                                        "teamType": "Group",
                                        "description": f"Entité propriétaire : {display}"})["id"]
    om.put("/glossaries", {"name": GLOSSARY, "displayName": "Glossaire financier WABA",
                           "description": "Définitions des indicateurs financiers et "
                                          "réglementaires du groupe (BCEAO, CIMA, LBC/FT)"})
    for term, desc in GLOSSARY_TERMS.items():
        om.put("/glossaryTerms", {"glossary": GLOSSARY, "name": term,
                                  "displayName": term.replace("_", " "), "description": desc})
    return teams


def merge_tags(existing: list[dict], wanted: list[dict]) -> list[dict]:
    have = {t["tagFQN"] for t in existing or []}
    return list(existing or []) + [t for t in wanted if t["tagFQN"] not in have]


def document_gold(om: OM, teams: dict[str, str]) -> int:
    done = 0
    for table, (desc, team, regs, terms) in GOLD_DOCS.items():
        ent = om.by_name("tables", table_fqn("gold", table), "tags,owners,columns")
        if ent is None:
            log(warning="table absente d'OpenMetadata (pas encore créée ?)", table=table)
            continue
        wanted = [tag_label(f"{CLASSIF}.{r}") for r in regs] + \
                 [tag_label(f"{GLOSSARY}.{t}", "Glossary") for t in terms] + \
                 [tag_label("Tier.Tier1")]
        ops = [{"op": "add", "path": "/description", "value": desc},
               {"op": "add", "path": "/tags", "value": merge_tags(ent.get("tags"), wanted)},
               {"op": "add", "path": "/owners", "value": [{"id": teams[team], "type": "team"}]}]
        ops += pii_ops(ent, PII_SILVER)
        om.patch(f"/tables/{ent['id']}", ops)
        done += 1
    return done


def pii_ops(ent: dict, pii_cols: set[str]) -> list[dict]:
    ops = []
    for i, c in enumerate(ent.get("columns") or []):
        if c["name"] in pii_cols:
            ops.append({"op": "add", "path": f"/columns/{i}/tags",
                        "value": merge_tags(c.get("tags"), [tag_label("PII.Sensitive"),
                                                            tag_label(f"{CLASSIF}.RGPD")])})
    return ops


def tag_pii(om: OM) -> int:
    n = 0
    for schema, names, cols in (("bronze", EVENTS + REFERENTIALS, PII_BRONZE),
                                ("silver", EVENTS + REFERENTIALS
                                 + [f"rt_{d}" for d in EVENTS], PII_SILVER)):
        for t in names:
            ent = om.by_name("tables", table_fqn(schema, t), "columns,tags")
            if ent is None:
                continue
            ops = pii_ops(ent, cols)
            if ops:
                if schema == "bronze":
                    ops.append({"op": "add", "path": "/description", "value":
                                f"Données brutes validées ({t}) : contient des identifiants "
                                "clients / comptes EN CLAIR, accès restreint (group_admin)."})
                om.patch(f"/tables/{ent['id']}", ops)
                n += len(ops)
    return n


def ensure_raw_containers(om: OM) -> dict[str, str]:
    """Couche raw = objets CSV du bucket raw-landing (service de stockage MinIO)."""
    om.put("/services/storageServices", {
        "name": STORAGE_SERVICE, "serviceType": "S3",
        "description": "MinIO du lakehouse WABA (raw-landing, lakehouse, archive)",
        "connection": {"config": {"type": "S3", "awsConfig": {
            "awsRegion": "us-east-1",
            "endPointURL": "http://minio.ingestion.svc.cluster.local:9000"}}}})
    root = om.put("/containers", {"name": "raw-landing", "service": STORAGE_SERVICE,
                                  "description": "Zone d'atterrissage des fichiers CSV bruts "
                                                 "(<PAYS>/<flux>/ et referentials/<table>/)"})
    ids = {}
    for d in EVENTS + REFERENTIALS:
        prefix = f"referentials/{d}/" if d in REFERENTIALS else f"<PAYS>/{d}/"
        c = om.put("/containers", {"name": d, "service": STORAGE_SERVICE,
                                   "parent": {"id": root["id"], "type": "container"},
                                   "prefix": prefix, "fileFormats": ["csv"],
                                   "description": f"Fichiers CSV bruts {d} ({prefix})"})
        ids[d] = c["id"]
    return ids


def add_edge(om: OM, src: tuple[str, str], dst: tuple[str, str], desc: str) -> None:
    om.put("/lineage", {"edge": {"fromEntity": {"id": src[1], "type": src[0]},
                                 "toEntity": {"id": dst[1], "type": dst[0]},
                                 "lineageDetails": {"description": desc}}})


def build_lineage(om: OM, raw: dict[str, str]) -> int:
    def table(schema, name):
        ent = om.by_name("tables", table_fqn(schema, name))
        return ("table", ent["id"]) if ent else None

    def topic(name):
        ent = om.by_name("topics", f"{KAFKA_SERVICE}.{name}")
        return ("topic", ent["id"]) if ent else None

    edges = []
    for d in EVENTS + REFERENTIALS:
        edges.append((("container", raw[d]), table("bronze", d),
                      "Spark ingest.py (dag_ingest_raw) : validation, MERGE idempotent"))
    for s, sources in SILVER_SOURCES.items():
        for b in sources:
            edges.append((table("bronze", b), table("silver", s),
                          "Spark silver.py (dag_bronze_to_silver) : dédup, EUR, pseudonymisation"))
    for g, sources in GOLD_SOURCES.items():
        job = "regulatory_report.py (dag_regulatory_report)" if g.startswith("regulatory") \
            else "gold.py (dag_silver_to_gold)"
        for s in sources:
            edges.append((table("silver", s), table("gold", g), f"Spark {job}"))
    # Branche temps réel (Level 3)
    for d in EVENTS:
        edges.append((("container", raw[d]), topic(RAW_TOPICS[d]), "NiFi ListS3 -> PublishKafka"))
        edges.append((topic(RAW_TOPICS[d]), table("silver", f"rt_{d}"),
                      "Spark Streaming stream_raw_to_silver.py"))
        edges.append((topic(RAW_TOPICS[d]), topic(SILVER_TOPICS[d]),
                      "Spark Streaming stream_raw_to_silver.py"))
    for alert, sources in STREAM_ALERTS.items():
        for d in sources:
            edges.append((topic(SILVER_TOPICS[d]), table("gold", alert),
                          "Spark Streaming stream_silver_to_gold.py"))
    n = 0
    for src, dst, desc in edges:
        if src and dst:
            add_edge(om, src, dst, desc)
            n += 1
    return n


def govern() -> int:
    wait_ready()
    om = admin_login()
    teams = ensure_taxonomy(om)
    documented = document_gold(om, teams)
    pii = tag_pii(om)
    raw = ensure_raw_containers(om)
    edges = build_lineage(om, raw)
    log(status="SUCCESS", gold_tables_documented=documented, pii_column_tags=pii,
        lineage_edges=edges)
    if documented < 5:
        log(warning="moins de 5 tables Gold documentées : lancer le pipeline Level 2 puis "
                    "relancer le Job (make k8s-governance)")
    return 0


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else "govern"
    sys.exit({"prepare": prepare, "govern": govern}[cmd]())
