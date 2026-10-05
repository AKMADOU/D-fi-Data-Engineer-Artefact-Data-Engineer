#!/usr/bin/env bash
# Initialisation idempotente de Superset (Job Kubernetes superset-init).
set -euo pipefail
. /app/.venv/bin/activate 2>/dev/null || true

superset db upgrade
# Compte local de secours / propriétaire des objets importés (les utilisateurs passent par Keycloak)
superset fab create-admin --username "${SUPERSET_ADMIN_USERNAME:-admin}" --firstname WABA \
  --lastname Admin --email admin@waba.local --password "${SUPERSET_ADMIN_PASSWORD}" || true
superset init

# Trino doit répondre (l'import liste ses catalogues et schémas)
for i in $(seq 1 60); do
  python - <<'PY' && break
import os, socket, sys
s = socket.socket(); s.settimeout(3)
sys.exit(s.connect_ex((os.environ.get("TRINO_HOST", "trino.serving.svc.cluster.local"),
                       int(os.environ.get("TRINO_PORT", "8443")))))
PY
  echo "attente de Trino ($i/60)..."; sleep 10
done
python /app/bootstrap/import_dashboards.py
echo '{"status":"SUCCESS","component":"superset-init"}'
