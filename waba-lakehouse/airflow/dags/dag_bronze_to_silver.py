"""dag_bronze_to_silver — Bronze -> Silver (nettoyage, enrichissement, consolidation).

Déclenché automatiquement quand dag_ingest_raw publie l'asset `bronze`
(data-aware scheduling) ; déclenchable aussi à la main, pour un sous-ensemble de pays.

Transformations (spark/jobs/silver.py) : déduplication sur la clé métier,
normalisation, gestion des NULL, jointure avec les référentiels, conversion en EUR,
pseudonymisation (SHA-256 salé), mise en quarantaine des clés orphelines,
signalement des montants aberrants. Chaque table écrite est contrôlée (unicité).
"""
from __future__ import annotations

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

from waba.common import (BRONZE, COUNTRIES_XCOM, COUNTRY_PARAMS, DEFAULT_ARGS, EVENT_DATASETS,
                         SILVER, START_DATE, SparkJobOperator, resolve_countries)

REFERENTIALS = ["customers", "accounts", "branches", "products"]

with DAG(
    dag_id="dag_bronze_to_silver",
    description="Bronze -> Silver : qualité, enrichissement, EUR, pseudonymisation",
    schedule=[BRONZE],
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=COUNTRY_PARAMS,
    tags=["waba", "silver", "level2"],
    doc_md=__doc__,
) as dag:

    countries = resolve_countries()

    silver_referentials = SparkJobOperator(
        task_id="silver_referentials",
        job="silver.py",
        job_args=f"--tables {' '.join(REFERENTIALS)} --countries {COUNTRIES_XCOM}",
    )

    silver_transactions = SparkJobOperator(
        task_id="silver_transactions",
        job="silver.py",
        job_args=f"--tables {' '.join(EVENT_DATASETS)} --countries {COUNTRIES_XCOM}",
    )

    publish_silver = EmptyOperator(task_id="publish_silver", outlets=[SILVER])

    countries >> silver_referentials >> silver_transactions >> publish_silver
