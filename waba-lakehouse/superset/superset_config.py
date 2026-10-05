"""Configuration Superset — WABA Level 4 (monté par ConfigMap, lu via SUPERSET_CONFIG_PATH).

* Métadonnées dans PostgreSQL ; aucun secret ici : tout vient de l'environnement
  (Secret Kubernetes `superset-env`).
* Authentification : SSO OpenID Connect via Keycloak (realm « waba »).
  Rôles Keycloak -> rôles Superset :
      group_admin        -> Admin
      country_analyst    -> WABA_Analyst     (Trino filtre ses lignes sur son pays)
      compliance_officer -> WABA_Compliance  (datasets réglementaires uniquement)
      viewer             -> WABA_Viewer      (agrégats, sans donnée sensible)
* Les requêtes vers Trino sont exécutées sous l'identité de l'utilisateur
  (impersonation) : les règles d'accès de Trino s'appliquent aussi dans Superset.
"""
from __future__ import annotations

import base64
import json
import logging
import os

from flask_appbuilder.security.manager import AUTH_OAUTH

from superset.security import SupersetSecurityManager

log = logging.getLogger("waba.superset")


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"variable d'environnement manquante : {name}")
    return value


SECRET_KEY = env("SUPERSET_SECRET_KEY")
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://{env('SUPERSET_DB_USER', 'superset')}:{env('SUPERSET_DB_PASSWORD')}"
    f"@{env('SUPERSET_DB_HOST', 'superset-db')}:5432/{env('SUPERSET_DB_NAME', 'superset')}")

# Derrière l'Ingress (TLS terminé par nginx) : respecter X-Forwarded-Proto / Host
ENABLE_PROXY_FIX = True
PREFERRED_URL_SCHEME = "https"
SESSION_COOKIE_SECURE = True
SESSION_COOKIE_SAMESITE = "Lax"
WTF_CSRF_ENABLED = True
TALISMAN_ENABLED = False  # CSP gérée par l'Ingress en production

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": False,
}
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache", "CACHE_DEFAULT_TIMEOUT": 300}
DATA_CACHE_CONFIG = CACHE_CONFIG

# --------------------------------------------------------------------------- SSO Keycloak
# Le navigateur utilise l'URL publique (Ingress) ; Superset joint Keycloak en interne
# pour échanger le code contre un jeton (backchannel), sans sortir du cluster.
KC_PUBLIC = env("KEYCLOAK_PUBLIC_URL", "https://keycloak.waba.local").rstrip("/")
KC_INTERNAL = env("KEYCLOAK_INTERNAL_URL", "http://keycloak.governance.svc.cluster.local:8080").rstrip("/")
KC_REALM = env("KEYCLOAK_REALM", "waba")
_oidc_public = f"{KC_PUBLIC}/realms/{KC_REALM}/protocol/openid-connect"
_oidc_internal = f"{KC_INTERNAL}/realms/{KC_REALM}/protocol/openid-connect"

AUTH_TYPE = AUTH_OAUTH
OAUTH_PROVIDERS = [{
    "name": "keycloak",
    "icon": "fa-key",
    "token_key": "access_token",
    "remote_app": {
        "client_id": env("SUPERSET_OIDC_CLIENT_ID", "superset"),
        "client_secret": env("SUPERSET_OIDC_CLIENT_SECRET"),
        # PKCE (S256) : exigé par le client « superset » du realm Keycloak
        "client_kwargs": {"scope": "openid email profile", "code_challenge_method": "S256"},
        "api_base_url": f"{_oidc_internal}/",
        "access_token_url": f"{_oidc_internal}/token",
        "authorize_url": f"{_oidc_public}/auth",
        "jwks_uri": f"{_oidc_internal}/certs",
        "server_metadata_url": None,
    },
}]
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Public"   # aucun droit tant qu'aucun rôle n'est mappé
AUTH_ROLES_SYNC_AT_LOGIN = True           # un changement de rôle dans Keycloak s'applique au login
AUTH_ROLES_MAPPING = {
    "group_admin": ["Admin"],
    "country_analyst": ["WABA_Analyst"],
    "compliance_officer": ["WABA_Compliance"],
    "viewer": ["WABA_Viewer"],
}
PUBLIC_ROLE_LIKE = None


def _jwt_claims(token: str) -> dict:
    """Charge utile du jeton d'accès (reçu directement de Keycloak par le backchannel)."""
    payload = token.split(".")[1]
    payload += "=" * (-len(payload) % 4)
    return json.loads(base64.urlsafe_b64decode(payload))


class WabaSecurityManager(SupersetSecurityManager):
    def oauth_user_info(self, provider, response=None):
        if provider != "keycloak":
            return super().oauth_user_info(provider, response)
        claims = _jwt_claims(response["access_token"])
        roles = [r for r in claims.get("groups", []) if r in AUTH_ROLES_MAPPING]
        info = {"username": claims.get("preferred_username"),
                "email": claims.get("email") or f"{claims.get('preferred_username')}@waba.local",
                "first_name": claims.get("given_name", ""),
                "last_name": claims.get("family_name", ""),
                "role_keys": roles}
        log.info("login SSO %s rôles=%s", info["username"], roles)
        return info


CUSTOM_SECURITY_MANAGER = WabaSecurityManager
