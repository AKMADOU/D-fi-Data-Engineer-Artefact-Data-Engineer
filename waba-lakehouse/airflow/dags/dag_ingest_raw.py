"""dag_ingest_raw — raw-landing (MinIO) -> couche Bronze (Iceberg).

Planifié toutes les 15 minutes (surchargeable par WABA_INGEST_SCHEDULE). La première
tâche liste les nouveaux fichiers dans MinIO pour les pays du run : s'il n'y en a
aucun, le DAG s'arrête proprement (short-circuit) et les couches suivantes ne sont
pas déclenchées. Sinon : référentiels d'abord (les faits en dépendent), puis les 4
flux transactionnels, puis publication de l'asset `bronze`, qui déclenche
dag_bronze_to_silver.

Déclenchement manuel pour un sous-ensemble de pays :
    airflow dags trigger dag_ingest_raw --conf '{"countries": ["CI", "SN"]}'
"""
from __future__ import annotations

import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, Variable, task

from waba.common import (BRONZE, COUNTRIES_XCOM, COUNTRY_PARAMS, DEFAULT_ARGS, EVENT_DATASETS,
                         S3_CONN_ID, START_DATE, SparkJobOperator, resolve_countries)

with DAG(
    dag_id="dag_ingest_raw",
    description="Ingestion raw-landing -> bronze.* (8 pays)",
    schedule=os.environ.get("WABA_INGEST_SCHEDULE", "*/15 * * * *"),
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=COUNTRY_PARAMS,
    tags=["waba", "bronze", "level2"],
    doc_md=__doc__,
) as dag:

    countries = resolve_countries()

    @task.short_circuit
    def detect_new_files(countries: list[str]) -> bool:
        """Compte les CSV en attente dans raw-landing (par pays + référentiels)."""
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        bucket = Variable.get("landing_bucket", default="raw-landing")
        pending = {p.rstrip("/"): len([k for k in hook.list_keys(bucket, prefix=p) or []
                                       if k.endswith(".csv")])
                   for p in [f"{cc}/" for cc in countries] + ["referentials/"]}
        print(f"fichiers en attente : {pending}")
        return sum(pending.values()) > 0

    new_files = detect_new_files(countries)

    ingest_referentials = SparkJobOperator(
        task_id="ingest_referentials",
        job="ingest.py",
        job_args="--dataset referentials --namespace bronze",
    )

    ingest_events = [
        SparkJobOperator(
            task_id=f"ingest_{dataset}",
            job="ingest.py",
            job_args=f"--dataset {dataset} --namespace bronze --countries {COUNTRIES_XCOM}",
        )
        for dataset in EVENT_DATASETS
    ]

    publish_bronze = EmptyOperator(task_id="publish_bronze", outlets=[BRONZE])

    countries >> new_files >> ingest_referentials >> ingest_events >> publish_bronze
