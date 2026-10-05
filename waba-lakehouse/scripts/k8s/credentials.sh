#!/usr/bin/env bash
# Affiche les identifiants de démonstration (lus dans k8s/secrets.env et .env, hors git).
set -euo pipefail
cd "$(dirname "$0")/../.."
get() { grep -E "^$1=" "$2" | head -1 | cut -d= -f2-; }
S=k8s/secrets.env; E=.env
cat <<TXT
SSO Keycloak (Superset, Trino) — mot de passe commun : $(get WABA_DEMO_PASSWORD $S)
  admin.groupe (group_admin) · analyste.ci (country_analyst, CI) · analyste.gh (country_analyst, GH)
  conformite (compliance_officer) · lecteur (viewer)
Console Keycloak  https://keycloak.waba.local/admin   admin / $(get KEYCLOAK_ADMIN_PASSWORD $S)
Superset (local)  https://superset.waba.local         admin / $(get SUPERSET_ADMIN_PASSWORD $S)  (secours ; les utilisateurs passent par Keycloak)
Grafana           https://grafana.waba.local          admin / $(get GRAFANA_ADMIN_PASSWORD $S)
Airflow           https://airflow.waba.local          admin / $(get AIRFLOW_ADMIN_PASSWORD $E)
NiFi              https://nifi.waba.local/nifi        $(get NIFI_USERNAME $E) / $(get NIFI_PASSWORD $E)
MinIO             https://minio.waba.local            $(get MINIO_ROOT_USER $E) / $(get MINIO_ROOT_PASSWORD $E)
OpenMetadata      https://openmetadata.waba.local     admin@open-metadata.org / admin  (à changer au 1er login)
Trino (JDBC)      jdbc:trino://trino.waba.local:443/iceberg?SSL=true&SSLVerification=NONE&externalAuthentication=true
TXT
