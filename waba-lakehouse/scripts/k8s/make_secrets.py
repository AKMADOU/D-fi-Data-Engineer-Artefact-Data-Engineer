#!/usr/bin/env python3
"""Prépare les Secrets Kubernetes du Level 4, HORS dépôt git (k8s/.generated/).

Sources :
  .env               variables des Levels 1-3 (MinIO, Airflow, NiFi, PII_HASH_SECRET)
  k8s/secrets.env    variables du Level 4 ; créé au premier lancement avec des valeurs
                     ALÉATOIRES (secrets.token_urlsafe) si absent — jamais versionné.
Sorties (lues par le kustomization.yaml racine) :
  k8s/.generated/<secret>.env        un fichier par Secret, avec les seules clés utiles
  k8s/.generated/tls.crt|key         certificat auto-signé *.waba.local (Ingress)
  k8s/.generated/trino.pem           clé + certificat pour le HTTPS interne de Trino
  k8s/.generated/password.db         comptes de service Trino (PBKDF2-HMAC-SHA256)
  k8s/.generated/passwords.json      compte admin Airflow (simple auth manager)
  k8s/.generated/trino-password      mot de passe du compte « prometheus » (scrape)
  k8s/.generated/profile/           Component Kustomize du profil de ressources
    python3 scripts/k8s/make_secrets.py [--profile full|light]
"""
from __future__ import annotations

import hashlib
import json
import os
import secrets
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "k8s" / ".generated"
L4_ENV = ROOT / "k8s" / "secrets.env"
DOMAIN = "waba.local"
HOSTS = ["minio", "streamlit", "nifi", "airflow", "trino", "superset", "keycloak",
         "openmetadata", "grafana"]

L4_KEYS = [
    ("KEYCLOAK_ADMIN_PASSWORD", "Administrateur de la console Keycloak (utilisateur admin)"),
    ("WABA_DEMO_PASSWORD", "Mot de passe des 5 comptes de démonstration (12 caractères min.)"),
    ("SUPERSET_ADMIN_PASSWORD", "Compte local de secours de Superset (admin)"),
    ("SUPERSET_SECRET_KEY", "Clé de signature des sessions Superset"),
    ("SUPERSET_DB_PASSWORD", "PostgreSQL de Superset"),
    ("SUPERSET_OIDC_CLIENT_SECRET", "Secret du client OIDC « superset » dans Keycloak"),
    ("TRINO_OIDC_CLIENT_SECRET", "Secret du client OIDC « trino » dans Keycloak"),
    ("TRINO_SHARED_SECRET", "Secret de communication interne de Trino"),
    ("TRINO_SUPERSET_PASSWORD", "Compte de service Trino « superset »"),
    ("TRINO_PROMETHEUS_PASSWORD", "Compte de service Trino « prometheus » (/metrics)"),
    ("TRINO_OPENMETADATA_PASSWORD", "Compte de service Trino « openmetadata »"),
    ("GOVERNANCE_DB_PASSWORD", "Superutilisateur PostgreSQL de la gouvernance"),
    ("KEYCLOAK_DB_PASSWORD", "Base PostgreSQL de Keycloak"),
    ("OPENMETADATA_DB_PASSWORD", "Base PostgreSQL d'OpenMetadata"),
    ("GRAFANA_ADMIN_PASSWORD", "Administrateur Grafana (utilisateur admin)"),
]
L13_REQUIRED = ["MINIO_ROOT_USER", "MINIO_ROOT_PASSWORD", "LAKEHOUSE_S3_ACCESS_KEY",
                "LAKEHOUSE_S3_SECRET_KEY", "PII_HASH_SECRET", "AIRFLOW_ADMIN_PASSWORD",
                "AIRFLOW_DB_PASSWORD", "AIRFLOW_FERNET_KEY", "AIRFLOW_JWT_SECRET",
                "AIRFLOW_API_SECRET_KEY", "NIFI_USERNAME", "NIFI_PASSWORD",
                "NIFI_SENSITIVE_PROPS_KEY"]


def read_env(path: Path) -> dict[str, str]:
    env = {}
    if path.exists():
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def ensure_l4_env() -> dict[str, str]:
    env = read_env(L4_ENV)
    missing = [(k, d) for k, d in L4_KEYS if not env.get(k)]
    if missing:
        with L4_ENV.open("a") as f:
            if not env:
                f.write("# Secrets du Level 4 — générés aléatoirement par scripts/k8s/"
                        "make_secrets.py.\n# NE PAS VERSIONNER (ignoré par git). "
                        "Modifiables : relancer make k8s-secrets.\n")
            for k, desc in missing:
                f.write(f"# {desc}\n{k}={secrets.token_urlsafe(24)}\n")
        L4_ENV.chmod(0o600)
        print(f"{len(missing)} secret(s) généré(s) dans {L4_ENV.relative_to(ROOT)}")
    return read_env(L4_ENV)


def pbkdf2_entry(user: str, password: str, iterations: int = 5000) -> str:
    """Format du fichier de mots de passe Trino : user:iterations:salt_hex:hash_hex."""
    salt = secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode(), salt, iterations, dklen=64)
    return f"{user}:{iterations}:{salt.hex()}:{digest.hex()}"


def write_env(name: str, values: dict[str, str]) -> None:
    lines = []
    for k, v in values.items():
        if "\n" in v:
            raise ValueError(f"{k} : valeur multi-lignes interdite")
        lines.append(f"{k}={v}")
    path = OUT / f"{name}.env"
    path.write_text("\n".join(lines) + "\n")
    path.chmod(0o600)


def make_tls() -> None:
    if (OUT / "tls.crt").exists() and (OUT / "tls.key").exists():
        return
    if not shutil.which("openssl"):
        raise SystemExit("openssl introuvable : nécessaire pour le certificat TLS")
    san = ",".join([f"DNS:*.{DOMAIN}", f"DNS:{DOMAIN}"]
                   + [f"DNS:{h}.{DOMAIN}" for h in HOSTS]
                   + ["DNS:trino.serving.svc.cluster.local", "DNS:trino.serving.svc",
                      "DNS:localhost"])
    subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-sha256", "-nodes",
                    "-days", "825", "-subj", f"/CN=*.{DOMAIN}/O=WABA Group",
                    "-addext", f"subjectAltName={san}",
                    "-keyout", str(OUT / "tls.key"), "-out", str(OUT / "tls.crt")],
                   check=True, capture_output=True)
    print("certificat TLS auto-signé *.waba.local généré")


def write_profile(argv: list[str]) -> str:
    """Component Kustomize du profil de ressources : « light » (poste 16 Go) ou « full »."""
    comp = OUT / "profile"
    comp.mkdir(parents=True, exist_ok=True)
    marker = comp / "name"
    name = argv[argv.index("--profile") + 1] if "--profile" in argv else \
        (marker.read_text().strip() if marker.exists() else "full")
    if name not in ("full", "light"):
        raise SystemExit(f"profil inconnu : {name} (full | light)")
    patches = []
    if name == "light":
        shutil.copy(ROOT / "k8s/profiles/light/patches.yaml", comp / "patches.yaml")
        patches = ["  - path: patches.yaml"]
    (comp / "kustomization.yaml").write_text(
        "# Généré par scripts/k8s/make_secrets.py — profil " + name + "\n"
        "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\n"
        # label du profil sur chaque ressource (un Component ne peut pas être vide)
        "commonAnnotations:\n  waba/resource-profile: " + name + "\n"
        + ("patches:\n" + "\n".join(patches) + "\n" if patches else ""))
    marker.write_text(name)
    return name


def main() -> int:
    OUT.mkdir(parents=True, exist_ok=True)
    profile = write_profile(sys.argv[1:])
    base = read_env(ROOT / ".env")
    missing = [k for k in L13_REQUIRED if not base.get(k)]
    if missing:
        print(f".env incomplet (manque {missing}) : cp .env.example .env && make env-upgrade",
              file=sys.stderr)
        return 2
    s = ensure_l4_env()
    for k in ("WABA_DEMO_PASSWORD", "NIFI_PASSWORD"):
        value = s.get(k) or base.get(k, "")
        if len(value) < 12:
            print(f"{k} doit faire au moins 12 caractères", file=sys.stderr)
            return 2

    minio = "http://minio.ingestion.svc.cluster.local:9000"
    s3_key, s3_secret = base["LAKEHOUSE_S3_ACCESS_KEY"], base["LAKEHOUSE_S3_SECRET_KEY"]
    write_env("ingestion-env", {
        "MINIO_ROOT_USER": base["MINIO_ROOT_USER"], "MINIO_ROOT_PASSWORD": base["MINIO_ROOT_PASSWORD"],
        "LAKEHOUSE_S3_ACCESS_KEY": s3_key, "LAKEHOUSE_S3_SECRET_KEY": s3_secret,
        "S3_ACCESS_KEY": s3_key, "S3_SECRET_KEY": s3_secret,
        "NIFI_USERNAME": base["NIFI_USERNAME"], "NIFI_PASSWORD": base["NIFI_PASSWORD"],
        "NIFI_SENSITIVE_PROPS_KEY": base["NIFI_SENSITIVE_PROPS_KEY"]})
    write_env("waba-spark-env", {
        "S3_ENDPOINT": minio, "S3_ACCESS_KEY": s3_key, "S3_SECRET_KEY": s3_secret,
        "AWS_REGION": base.get("AWS_REGION", "us-east-1"),
        "ICEBERG_REST_URI": "http://iceberg-rest.processing.svc.cluster.local:8181",
        "ICEBERG_WAREHOUSE": "s3://lakehouse/", "PII_HASH_SECRET": base["PII_HASH_SECRET"],
        "LANDING_BUCKET": "raw-landing", "ARCHIVE_BUCKET": "archive",
        "REPORTS_BUCKET": "regulatory-reports"})
    conn = {"conn_type": "aws", "login": s3_key, "password": s3_secret,
            "extra": {"endpoint_url": minio, "region_name": base.get("AWS_REGION", "us-east-1"),
                      "config_kwargs": {"s3": {"addressing_style": "path"}}}}
    write_env("airflow-env", {
        "POSTGRES_PASSWORD": base["AIRFLOW_DB_PASSWORD"],
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN":
            f"postgresql+psycopg2://airflow:{base['AIRFLOW_DB_PASSWORD']}@airflow-db:5432/airflow",
        "AIRFLOW__CORE__FERNET_KEY": base["AIRFLOW_FERNET_KEY"],
        "AIRFLOW__API_AUTH__JWT_SECRET": base["AIRFLOW_JWT_SECRET"],
        "AIRFLOW__API__SECRET_KEY": base["AIRFLOW_API_SECRET_KEY"],
        "AIRFLOW_ADMIN_PASSWORD": base["AIRFLOW_ADMIN_PASSWORD"],
        "AIRFLOW_CONN_MINIO_S3": json.dumps(conn, separators=(",", ":")),
        "AIRFLOW_VAR_PII_HASH_SECRET": base["PII_HASH_SECRET"],
        "AIRFLOW_VAR_ALERT_WEBHOOK_URL": base.get("ALERT_WEBHOOK_URL", "")})
    (OUT / "passwords.json").write_text(json.dumps({"admin": base["AIRFLOW_ADMIN_PASSWORD"]}))
    write_env("trino-env", {
        "S3_ACCESS_KEY": s3_key, "S3_SECRET_KEY": s3_secret,
        "TRINO_OIDC_CLIENT_SECRET": s["TRINO_OIDC_CLIENT_SECRET"],
        "TRINO_SHARED_SECRET": s["TRINO_SHARED_SECRET"]})
    (OUT / "password.db").write_text("\n".join([
        pbkdf2_entry("superset", s["TRINO_SUPERSET_PASSWORD"]),
        pbkdf2_entry("prometheus", s["TRINO_PROMETHEUS_PASSWORD"]),
        pbkdf2_entry("openmetadata", s["TRINO_OPENMETADATA_PASSWORD"])]) + "\n")
    write_env("superset-env", {
        "SUPERSET_SECRET_KEY": s["SUPERSET_SECRET_KEY"],
        "SUPERSET_DB_PASSWORD": s["SUPERSET_DB_PASSWORD"],
        "SUPERSET_OIDC_CLIENT_SECRET": s["SUPERSET_OIDC_CLIENT_SECRET"],
        "SUPERSET_ADMIN_PASSWORD": s["SUPERSET_ADMIN_PASSWORD"],
        "TRINO_SUPERSET_PASSWORD": s["TRINO_SUPERSET_PASSWORD"]})
    write_env("governance-env", {
        "POSTGRES_PASSWORD": s["GOVERNANCE_DB_PASSWORD"],
        "KC_DB_PASSWORD": s["KEYCLOAK_DB_PASSWORD"],
        "DB_USER_PASSWORD": s["OPENMETADATA_DB_PASSWORD"],
        "TRINO_OPENMETADATA_PASSWORD": s["TRINO_OPENMETADATA_PASSWORD"]})
    write_env("keycloak-env", {
        "KC_DB_PASSWORD": s["KEYCLOAK_DB_PASSWORD"],
        "KC_BOOTSTRAP_ADMIN_PASSWORD": s["KEYCLOAK_ADMIN_PASSWORD"],
        "SUPERSET_OIDC_CLIENT_SECRET": s["SUPERSET_OIDC_CLIENT_SECRET"],
        "TRINO_OIDC_CLIENT_SECRET": s["TRINO_OIDC_CLIENT_SECRET"],
        "WABA_DEMO_PASSWORD": s["WABA_DEMO_PASSWORD"]})
    write_env("grafana-env", {"GF_SECURITY_ADMIN_PASSWORD": s["GRAFANA_ADMIN_PASSWORD"]})
    (OUT / "trino-password").write_text(s["TRINO_PROMETHEUS_PASSWORD"])
    make_tls()
    (OUT / "trino.pem").write_text((OUT / "tls.key").read_text() + (OUT / "tls.crt").read_text())
    for f in OUT.iterdir():
        if f.is_file():
            f.chmod(0o600)
    print(f"Secrets prêts dans {OUT.relative_to(ROOT)}/ (ignoré par git) — profil {profile}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
