#!/usr/bin/env bash
# Déclenche un DAG Airflow et attend la fin de son exécution.
#   ./scripts/airflow_run.sh dag_ingest_raw
#   ./scripts/airflow_run.sh dag_regulatory_report '{"report_date": "2026-09-16"}'
#   ./scripts/airflow_run.sh --wait-only dag_bronze_to_silver   # run déclenché par un asset
set -euo pipefail
cd "$(dirname "$0")/.."
# AIRFLOW_EXEC permet de viser Kubernetes (Level 4) :
#   AIRFLOW_EXEC="kubectl -n processing exec deploy/airflow-scheduler -- airflow"
if [ -n "${AIRFLOW_EXEC:-}" ]; then read -r -a AF <<< "$AIRFLOW_EXEC"; else AF=(docker compose exec -T airflow-apiserver airflow); fi
TIMEOUT="${TIMEOUT:-1800}"

wait_only=false
if [ "${1:-}" = "--wait-only" ]; then wait_only=true; shift; fi
DAG="$1"; CONF="${2:-}"; [ -n "$CONF" ] || CONF="{}"
# Runs pris en compte : ceux créés après SINCE (défaut : lancement de ce script).
# Pour une chaîne déclenchée par assets, passer l'heure de début de la chaîne.
SINCE="${SINCE:-$(date -u +%Y-%m-%dT%H:%M:%S)}"

latest_run() {  # "run_id state" du run le plus récent créé après SINCE
  "${AF[@]}" dags list-runs "$DAG" -o json 2>/dev/null | SINCE="$SINCE" python3 -c '
import json, os, sys
lines = [l for l in sys.stdin.read().splitlines() if l.lstrip().startswith("[")]
runs = json.loads(lines[-1]) if lines else []
runs = [r for r in runs if (r.get("run_after") or "")[:19] >= os.environ["SINCE"]]
runs.sort(key=lambda r: r.get("run_after") or "", reverse=True)
print(runs[0]["run_id"], runs[0]["state"]) if runs else print("none none")'
}

if ! $wait_only; then
  "${AF[@]}" dags unpause "$DAG" >/dev/null
  "${AF[@]}" dags trigger "$DAG" --conf "$CONF" >/dev/null
  echo "▶ $DAG déclenché (conf=$CONF)"
fi

start=$(date +%s)
while true; do
  read -r run state <<< "$(latest_run)"
  if [ "$run" != "none" ]; then
    case "$state" in
      success) echo "✔ $DAG : $run -> success"; exit 0 ;;
      failed)  echo "✘ $DAG : $run -> failed (voir http://localhost:8090)" >&2; exit 1 ;;
    esac
  fi
  (( $(date +%s) - start > TIMEOUT )) && { echo "⏱ timeout $DAG" >&2; exit 2; }
  printf '.'; sleep 10
done
