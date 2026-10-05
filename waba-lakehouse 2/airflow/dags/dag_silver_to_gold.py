"""dag_silver_to_gold — Silver -> Gold : 7 tables de KPIs financiers et réglementaires.

Déclenché par la publication de l'asset `silver`. Trois tâches indépendantes, une
par domaine métier (tables distinctes, donc parallélisables sans conflit d'écriture) :

* banking      : daily_transaction_volume, npl_ratio_by_country, customer_arpu_monthly
* insurance    : loss_ratio_by_product, claims_processing_time
* mobile_money : mobile_money_daily_flow, cross_border_transfers

Chaque tâche remplace uniquement les partitions des pays du run (idempotent).
"""
from __future__ import annotations

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG

from waba.common import (COUNTRIES_XCOM, COUNTRY_PARAMS, DEFAULT_ARGS, GOLD, SILVER, START_DATE,
                         SparkJobOperator, resolve_countries)

DOMAINS = ["banking", "insurance", "mobile_money"]

with DAG(
    dag_id="dag_silver_to_gold",
    description="Silver -> Gold : KPIs bancaires, assurance, mobile money",
    schedule=[SILVER],
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    params=COUNTRY_PARAMS,
    tags=["waba", "gold", "kpi", "level2"],
    doc_md=__doc__,
) as dag:

    countries = resolve_countries()

    gold_tasks = [
        SparkJobOperator(
            task_id=f"gold_{domain}",
            job="gold.py",
            job_args=f"--domain {domain} --countries {COUNTRIES_XCOM}",
        )
        for domain in DOMAINS
    ]

    publish_gold = EmptyOperator(task_id="publish_gold", outlets=[GOLD])

    countries >> gold_tasks >> publish_gold
