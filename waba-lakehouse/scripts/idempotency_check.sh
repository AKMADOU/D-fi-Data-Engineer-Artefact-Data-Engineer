#!/usr/bin/env bash
# Démontre l'idempotence : on rejoue TOUS les fichiers déjà archivés (--source archive,
# qui implique --force) et on vérifie que le nombre de lignes des tables bronze.* ne change pas.
set -euo pipefail
cd "$(dirname "$0")/.."

TABLES="customers accounts branches products bank_transactions insurance_operations mobile_money_payments loan_repayments"
count_all() {
  local q=""
  for t in $TABLES; do q="${q}${q:+ UNION ALL }SELECT '$t', count(*) FROM iceberg.bronze.$t"; done
  docker compose exec -T trino trino --output-format TSV --execute "$q ORDER BY 1"
}

echo "== Comptages AVANT rejeu"; before="$(count_all)"; echo "$before"
echo "== Rejeu des fichiers archivés (MERGE idempotent)"
./scripts/spark-submit.sh ingest.py --dataset all --source archive --namespace bronze
echo "== Comptages APRÈS rejeu"; after="$(count_all)"; echo "$after"

if [ "$before" == "$after" ]; then
  echo "OK : aucun doublon créé par le rejeu."
else
  echo "ÉCHEC : les comptages diffèrent." >&2; exit 1
fi
