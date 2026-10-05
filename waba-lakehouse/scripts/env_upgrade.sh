#!/usr/bin/env bash
# Ajoute à .env les variables présentes dans .env.example mais absentes de .env
# (mise à niveau Level 1 -> Level 2 sans écraser vos valeurs existantes).
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || { cp .env.example .env; echo ".env créé depuis .env.example"; exit 0; }
added=0
while IFS= read -r line; do
  [[ "$line" =~ ^[A-Z_][A-Z0-9_]*= ]] || continue
  key="${line%%=*}"
  if ! grep -qE "^${key}=" .env; then
    [ $added -eq 0 ] && printf '\n# --- ajouté par env_upgrade.sh (%s)\n' "$(date +%F)" >> .env
    echo "$line" >> .env; echo "  + $key"; added=$((added + 1))
  fi
done < .env.example
echo "$added variable(s) ajoutée(s) à .env"
