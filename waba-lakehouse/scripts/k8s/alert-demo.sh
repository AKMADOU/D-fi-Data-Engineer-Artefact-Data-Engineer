#!/usr/bin/env bash
# Déclenche volontairement les 3 alertes Grafana (critère « déclenchées au moins une fois »).
#   ./scripts/k8s/alert-demo.sh regulatory   échec de dag_regulatory_report  -> immédiat
#   ./scripts/k8s/alert-demo.sh lag          lag AML > 5 000 messages         -> ~2 min
#   ./scripts/k8s/alert-demo.sh fraud-down   job fraude arrêté > 5 min        -> ~6 min
#   ./scripts/k8s/alert-demo.sh restore      remet la situation normale
# Suivi : Grafana > Alerting > Alert rules, et kubectl -n monitoring logs deploy/alert-webhook
set -euo pipefail
AF="kubectl -n processing exec deploy/airflow-scheduler -- airflow"
case "${1:-}" in
  regulatory)
    # Date invalide : le job Spark échoue -> callback -> Pushgateway (last_failure > last_success)
    $AF dags unpause dag_regulatory_report >/dev/null
    $AF dags trigger dag_regulatory_report --conf '{"report_date": "1999-01-01"}'
    echo "L'alerte se déclenche dès l'échec final du DAG (après les retries, ~5-10 min)." ;;
  lag)
    # Le consommateur AML ne lit plus que 200 messages par micro-lot, puis on injecte 20 000 paiements
    kubectl -n processing set env deploy/stream-gold STREAM_EVENTS_MAX_OFFSETS=200
    kubectl -n processing rollout status deploy/stream-gold --timeout=600s
    kubectl -n ingestion exec deploy/generator -- python -m waba_gen.cli continuous \
      --dataset mobile_money_payments --rows 20000 --iterations 1 --countries CI SN
    echo "NiFi -> raw -> Job 1 -> silver-mobile-money : le lag de event_rules dépasse 5 000." ;;
  fraud-down)
    kubectl -n processing scale deploy/stream-gold --replicas=0
    echo "stream-gold arrêté : l'alerte passe « Firing » après 5 min (pending avant)." ;;
  restore)
    kubectl -n processing set env deploy/stream-gold STREAM_EVENTS_MAX_OFFSETS=20000
    kubectl -n processing scale deploy/stream-gold --replicas=1 ;;
  *) sed -n '2,8p' "$0"; exit 1 ;;
esac
