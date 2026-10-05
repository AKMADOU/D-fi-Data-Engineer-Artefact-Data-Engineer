#!/usr/bin/env bash
# État des pods par namespace, Jobs d'initialisation et Ingress.
set -uo pipefail
for ns in ingestion processing serving governance monitoring; do
  echo "== $ns"; kubectl -n "$ns" get pods -o wide --no-headers 2>/dev/null \
    | awk '{printf "  %-45s %-8s %-18s %s\n", $1, $2, $3, $4}'
done
echo "== Jobs"; kubectl get jobs -A -l app.kubernetes.io/part-of=waba 2>/dev/null
echo "== Ingress"; kubectl get ingress -A 2>/dev/null
