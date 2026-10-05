#!/usr/bin/env bash
# Soumet un job PySpark au cluster (driver en client mode dans spark-master).
#   ./scripts/spark-submit.sh ingest.py --dataset all
set -euo pipefail
JOB="$1"; shift
exec docker compose exec -T spark-master spark-submit \
  --conf spark.driver.host=spark-master \
  "/opt/waba/jobs/${JOB}" "$@"
