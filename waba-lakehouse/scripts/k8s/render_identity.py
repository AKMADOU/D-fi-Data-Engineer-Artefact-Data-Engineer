#!/usr/bin/env python3
"""Génère, depuis k8s/users.json, les fichiers d'identité et d'autorisation (Level 4) :

  k8s/governance/keycloak/waba-realm.json   realm Keycloak (rôles, clients OIDC, utilisateurs)
  k8s/serving/trino/group.txt              groupes Trino (mêmes rôles que Keycloak)
  k8s/serving/trino/rules.json             contrôle d'accès Trino (filtres pays, masques)

Aucun secret : le realm contient des placeholders ${VAR} que Keycloak remplace au
démarrage par les variables d'environnement du Secret `keycloak-env`.
    python3 scripts/k8s/render_identity.py          # régénère (fichiers versionnés)
    python3 scripts/k8s/render_identity.py --check  # échoue si les fichiers ne sont pas à jour
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
K8S = ROOT / "k8s"
DOMAIN = "waba.local"

BUSINESS_ROLES = {
    "group_admin": "Accès à toutes les entités et tous les pays",
    "country_analyst": "Analyste restreint à son pays (rôle country_XX associé)",
    "compliance_officer": "Accès aux seules tables Gold réglementaires (BCEAO, CIMA, AML)",
    "viewer": "Lecture des agrégats, sans donnée sensible",
}
GOLD_KPIS = ["daily_transaction_volume", "npl_ratio_by_country", "customer_arpu_monthly",
             "loss_ratio_by_product", "claims_processing_time", "mobile_money_daily_flow",
             "cross_border_transfers"]
REGULATORY = ["regulatory_bceao_daily", "regulatory_cima_daily", "aml_events", "fraud_alerts",
              "liquidity_alerts", "npl_ratio_by_country", "loss_ratio_by_product",
              "claims_processing_time"]
# Colonnes pseudonymisées (données personnelles) masquées au rôle viewer
PII_KEYS = ["account_key", "customer_key", "beneficiary_account_key", "sender_key",
            "receiver_key"]


def load() -> dict:
    return json.loads((K8S / "users.json").read_text())


def groups_claim_mapper() -> dict:
    return {"name": "groups (rôles du realm)", "protocol": "openid-connect",
            "protocolMapper": "oidc-usermodel-realm-role-mapper", "consentRequired": False,
            "config": {"multivalued": "true", "claim.name": "groups", "jsonType.label": "String",
                       "id.token.claim": "true", "access.token.claim": "true",
                       "userinfo.token.claim": "true", "introspection.token.claim": "true"}}


def audience_mapper(client: str) -> dict:
    return {"name": f"audience {client}", "protocol": "openid-connect",
            "protocolMapper": "oidc-audience-mapper", "consentRequired": False,
            "config": {"included.client.audience": client, "access.token.claim": "true",
                       "id.token.claim": "false", "introspection.token.claim": "true"}}


def client(client_id: str, secret_var: str, redirects: list[str], name: str,
           pkce: bool = True) -> dict:
    base = redirects[0].split("/", 3)[:3]
    attributes = {"post.logout.redirect.uris": "+"}
    if pkce:
        attributes["pkce.code.challenge.method"] = "S256"
    return {"clientId": client_id, "name": name, "enabled": True, "protocol": "openid-connect",
            "publicClient": False, "clientAuthenticatorType": "client-secret",
            "secret": "${" + secret_var + "}", "standardFlowEnabled": True,
            "directAccessGrantsEnabled": False, "serviceAccountsEnabled": False,
            "implicitFlowEnabled": False, "redirectUris": redirects,
            "webOrigins": ["/".join(base)], "attributes": attributes,
            "protocolMappers": [groups_claim_mapper(), audience_mapper(client_id)]}


def realm(cfg: dict) -> dict:
    roles = [{"name": r, "description": d} for r, d in BUSINESS_ROLES.items()]
    roles += [{"name": f"country_{c}", "description": f"Périmètre pays {c}"}
              for c in cfg["countries"]]
    users = [{"username": u["username"], "enabled": True, "emailVerified": True,
              "firstName": u["firstName"], "lastName": u["lastName"],
              "email": f"{u['username']}@{DOMAIN}",
              "credentials": [{"type": "password", "value": "${WABA_DEMO_PASSWORD}",
                               "temporary": False}],
              "realmRoles": u["roles"]} for u in cfg["users"]]
    return {
        "realm": "waba", "displayName": "WABA Group", "enabled": True,
        "sslRequired": "external", "loginWithEmailAllowed": True, "registrationAllowed": False,
        "bruteForceProtected": True, "accessTokenLifespan": 900,
        "ssoSessionIdleTimeout": 3600, "passwordPolicy": "length(12)",
        "roles": {"realm": roles},
        "clients": [
            client("superset", "SUPERSET_OIDC_CLIENT_SECRET",
                   [f"https://superset.{DOMAIN}/oauth-authorized/keycloak"], "Apache Superset"),
            # Trino n'implémente pas PKCE : client confidentiel (secret) sans PKCE
            client("trino", "TRINO_OIDC_CLIENT_SECRET",
                   [f"https://trino.{DOMAIN}/oauth2/callback"], "Trino", pkce=False),
        ],
        "users": users,
    }


def group_file(cfg: dict) -> str:
    groups: dict[str, list[str]] = {}
    for u in cfg["users"]:
        for r in u["roles"]:
            groups.setdefault(r, []).append(u["username"])
    # Compte local « admin » de Superset (import des dashboards) : administrateur
    groups.setdefault("group_admin", []).append("admin")
    groups["service"] = sorted(cfg["service_accounts"])
    # Pas de commentaire : le format du fichier de groupes Trino est strictement groupe:u1,u2
    lines = [f"{g}:{','.join(sorted(set(users)))}" for g, users in sorted(groups.items())]
    return "\n".join(lines) + "\n"


def rules(cfg: dict) -> dict:
    ro = ["SELECT"]
    tables: list[dict] = [
        {"group": "group_admin", "privileges": ["SELECT", "INSERT", "DELETE", "UPDATE",
                                                "OWNERSHIP", "GRANT_SELECT"]},
        # Ingestion des métadonnées (pas de profilage ni d'échantillons côté OpenMetadata)
        {"user": "openmetadata", "catalog": "iceberg|kafka", "privileges": ro},
        {"group": "compliance_officer", "catalog": "iceberg", "schema": "gold",
         "table": "|".join(REGULATORY), "privileges": ro},
    ]
    for c in cfg["countries"]:
        tables.append({"group": f"country_{c}", "catalog": "iceberg", "schema": "silver",
                       "table": "fx_rates", "privileges": ro})
        tables.append({"group": f"country_{c}", "catalog": "iceberg", "schema": "silver|gold",
                       "privileges": ro, "filter": f"country_code = '{c}'"})
    masks = [{"name": k, "mask": "'***'"} for k in PII_KEYS]
    tables += [
        {"group": "viewer", "catalog": "iceberg", "schema": "gold",
         "table": "|".join(GOLD_KPIS), "privileges": ro},
        # Montants de sinistres individuels et identifiants masqués
        {"group": "viewer", "catalog": "iceberg", "schema": "silver",
         "table": "insurance_operations", "privileges": ro,
         "columns": masks + [
             {"name": "amount", "mask": "CASE WHEN is_claim THEN NULL ELSE amount END"},
             {"name": "amount_eur", "mask": "CASE WHEN is_claim THEN NULL ELSE amount_eur END"}]},
        {"group": "viewer", "catalog": "iceberg", "schema": "silver",
         "table": "bank_transactions|mobile_money_payments|loan_repayments|accounts",
         "privileges": ro, "columns": masks},
        {"privileges": []},     # tout le reste : refusé (dont bronze et audit)
    ]
    readers = "group_admin|country_analyst|compliance_officer|viewer"
    return {
        "catalogs": [
            {"group": "group_admin", "allow": "all"},
            {"user": "openmetadata", "catalog": "iceberg|kafka|system", "allow": "read-only"},
            {"group": readers, "catalog": "iceberg|system", "allow": "read-only"},
            {"group": "service", "catalog": "system", "allow": "read-only"},
            {"allow": "none"},
        ],
        "schemas": [
            {"group": "group_admin", "owner": True},
            {"user": "openmetadata", "owner": False},
            {"group": "country_analyst|viewer", "schema": "silver|gold|information_schema",
             "owner": False},
            {"group": "compliance_officer", "schema": "gold|information_schema", "owner": False},
            {"owner": False, "schema": "information_schema"},
        ],
        "tables": tables,
        # Superset exécute les requêtes sous l'identité de l'utilisateur SSO
        "impersonation": [
            {"original_user": "superset", "new_user": ".*", "allow": True},
            {"original_user": ".*", "new_user": ".*", "allow": False},
        ],
        "queries": [
            {"group": "group_admin", "allow": ["execute", "kill", "view"]},
            {"allow": ["execute"]},
        ],
        "system_information": [{"group": "group_admin|service", "allow": ["read"]}],
    }


def outputs() -> dict[Path, str]:
    cfg = load()
    return {
        K8S / "governance/keycloak/waba-realm.json":
            json.dumps(realm(cfg), indent=2, ensure_ascii=False) + "\n",
        K8S / "serving/trino/group.txt": group_file(cfg),
        K8S / "serving/trino/rules.json": json.dumps(rules(cfg), indent=2) + "\n",
    }


def main(argv: list[str]) -> int:
    stale = []
    for path, content in outputs().items():
        if "--check" in argv:
            if not path.exists() or path.read_text() != content:
                stale.append(str(path.relative_to(ROOT)))
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        print(f"écrit : {path.relative_to(ROOT)}")
    if stale:
        print(f"fichiers à régénérer : {stale}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
