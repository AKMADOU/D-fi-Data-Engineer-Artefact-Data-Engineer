#!/usr/bin/env bash
# Création idempotente des topics du Level 3 (conteneur one-shot kafka-init).
set -euo pipefail
BS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:9092}"
KT=/opt/kafka/bin/kafka-topics.sh
PARTITIONS="${KAFKA_TOPIC_PARTITIONS:-3}"

for i in $(seq 1 60); do
  $KT --bootstrap-server "$BS" --list >/dev/null 2>&1 && break
  echo "attente de Kafka ($i)..."; sleep 5
done

create() {  # nom, rétention (ms)
  $KT --bootstrap-server "$BS" --create --if-not-exists --topic "$1" \
      --partitions "$PARTITIONS" --replication-factor 1 \
      --config retention.ms="$2" --config cleanup.policy=delete
}
WEEK=604800000; MONTH=2592000000; QUARTER=7776000000
# Couche Raw (publiés par NiFi) : 7 jours, le batch conserve l'historique
for t in raw-bank-transactions raw-insurance-operations raw-mobile-money-payments raw-loan-repayments; do
  create "$t" "$WEEK"; done
# Couche Silver (Spark Streaming Job 1)
for t in silver-bank-transactions silver-insurance-operations silver-mobile-money silver-loan-repayments; do
  create "$t" "$WEEK"; done
# Couche Gold (Spark Streaming Job 2) : alertes conservées 90 jours (audit conformité)
for t in gold-fraud-alerts gold-aml-events gold-liquidity-alerts; do
  create "$t" "$QUARTER"; done
# Dead Letter Queue : 30 jours pour analyse / rejeu
create dlq-financial-events "$MONTH"

$KT --bootstrap-server "$BS" --list
echo '{"status":"SUCCESS","component":"kafka-init"}'
