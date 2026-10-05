#!/usr/bin/env bash
# Initialisation Airflow (idempotente) : schéma de la base, pool Spark, compte admin.
set -euo pipefail

airflow db migrate

# Un seul job Spark à la fois par défaut : dimensionné pour un poste de 8-16 Go.
# Augmenter le nombre de slots si le cluster Spark a plus de ressources.
airflow pools set spark "${SPARK_POOL_SLOTS:-1}" "Jobs Spark (conteneurs éphémères)"

# Simple auth manager (Airflow 3) : mot de passe admin fourni par l'environnement,
# jamais généré aléatoirement ni écrit dans le code.
python - <<'PY'
import json, os
path = os.environ["AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE"]
with open(path, "w") as f:
    json.dump({"admin": os.environ["AIRFLOW_ADMIN_PASSWORD"]}, f)
os.chmod(path, 0o600)
print(f"compte admin configuré ({path})")
PY
echo "Airflow initialisé"
