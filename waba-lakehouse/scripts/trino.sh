#!/usr/bin/env bash
# Trino CLI : ./scripts/trino.sh                      -> session interactive
#             ./scripts/trino.sh -f 02_analytics.sql  -> exécute un fichier de sql/
#             ./scripts/trino.sh -e "SELECT 1"        -> exécute une requête
set -euo pipefail
case "${1:-}" in
  -f) exec docker compose exec -T trino trino --catalog iceberg --schema bronze --file "/sql/$2" ;;
  -e) exec docker compose exec -T trino trino --catalog iceberg --schema bronze --output-format ALIGNED --execute "$2" ;;
  *)  exec docker compose exec trino trino --catalog iceberg --schema bronze ;;
esac
