#!/usr/bin/env bash
# Affiche la ligne à ajouter à /etc/hosts pour joindre les UIs par l'Ingress.
set -euo pipefail
HOSTS="minio streamlit nifi airflow trino superset keycloak openmetadata grafana"
IP="$(minikube -p "${MINIKUBE_PROFILE:-waba}" ip 2>/dev/null || echo 127.0.0.1)"
DRIVER="$(minikube profile list -o json 2>/dev/null | python3 -c "import json,sys,os;p=[x for x in json.load(sys.stdin)[\"valid\"] if x[\"Name\"]==os.environ.get(\"MINIKUBE_PROFILE\",\"waba\")];print(p[0][\"Config\"][\"Driver\"] if p else \"\")" 2>/dev/null || echo "")"
if [ "$(uname)" = "Darwin" ] && [ "$DRIVER" = "docker" ]; then
  IP=127.0.0.1
  echo "macOS + driver docker : lancer « minikube -p ${MINIKUBE_PROFILE:-waba} tunnel » dans un autre terminal (sudo)."
fi
LINE="$IP $(for h in $HOSTS; do printf '%s.waba.local ' "$h"; done)"
echo "Ajouter à /etc/hosts (sudo) :"
echo "  $LINE"
echo "Puis ouvrir https://superset.waba.local (certificat auto-signé : accepter l'alerte)."
