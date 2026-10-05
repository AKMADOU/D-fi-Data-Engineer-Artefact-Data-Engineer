#!/usr/bin/env bash
# Construit les 5 images du projet DANS minikube (pas de registre nécessaire).
set -euo pipefail
cd "$(dirname "$0")/../.."
build() {  # tag contexte
  echo "==> $1"
  minikube -p "${MINIKUBE_PROFILE:-waba}" image build -t "$1" "$2"
}
build waba/spark:3.5.3-iceberg1.6.1 spark
build waba/airflow:3.3.2 airflow
build waba/generator:1.0 generator
build waba/minio-init:1.0 minio
build waba/superset:6.1.0-waba1 superset
minikube -p "${MINIKUBE_PROFILE:-waba}" image ls | grep waba/
