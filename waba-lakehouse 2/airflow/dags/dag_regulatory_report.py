"""dag_regulatory_report — Agrégats réglementaires BCEAO / CIMA, quotidien à J+1 00h30 UTC.

La date de reporting D est la veille de l'exécution. Pour rejouer une date passée
(ex. démo sur les données générées du trimestre précédent) :
    airflow dags trigger dag_regulatory_report --conf '{"report_date": "2026-06-15"}'

Sorties : gold.regulatory_bceao_daily, gold.regulatory_cima_daily et les fichiers
de déclaration CSV dans s3://regulatory-reports/<bceao|cima>/report_date=D/country_code=XX/.
La dernière tâche vérifie que les fichiers de chaque pays ont bien été produits.
"""
from __future__ import annotations

from datetime import timedelta

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import DAG, Param, Variable, task

from waba.common import (COUNTRIES_XCOM, COUNTRY_PARAMS, DEFAULT_ARGS, S3_CONN_ID, START_DATE,
                         SparkJobOperator, notify_success, resolve_countries)

REPORT_DATE = "{{ params.report_date or macros.ds_add(ds, -1) }}"

with DAG(
    dag_id="dag_regulatory_report",
    description="Reporting réglementaire J+1 BCEAO / CIMA",
    schedule="30 0 * * *",            # tous les jours à 00h30 UTC
    start_date=START_DATE,
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 3, "execution_timeout": timedelta(minutes=45)},
    params={
        **COUNTRY_PARAMS,
        "report_date": Param(None, type=["null", "string"], format="date",
                             title="Date de reporting (YYYY-MM-DD)",
                             description="Vide = veille de l'exécution (J-1)."),
    },
    tags=["waba", "regulatory", "bceao", "cima", "level2"],
    doc_md=__doc__,
) as dag:

    countries = resolve_countries()

    regulatory_report = SparkJobOperator(
        task_id="build_regulatory_report",
        job="regulatory_report.py",
        job_args=f"--report-date {REPORT_DATE} --countries {COUNTRIES_XCOM}",
    )

    # Succès de la dernière tâche -> horodatage poussé au Pushgateway (alerte Grafana J+1 06h00)
    @task(on_success_callback=notify_success)
    def check_exports(countries: list[str], report_date: str) -> dict:
        """Contrôle de complétude : un fichier de déclaration par régulateur et par pays."""
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        bucket = Variable.get("reports_bucket", default="regulatory-reports")
        missing, found = [], {}
        for regulator in ("bceao", "cima"):
            keys = hook.list_keys(bucket, prefix=f"{regulator}/report_date={report_date}/") or []
            for cc in countries:
                files = [k for k in keys if f"country_code={cc}/" in k and k.endswith(".csv")]
                found[f"{regulator}/{cc}"] = len(files)
                # CIMA : pas de ligne si aucune opération d'assurance dans le mois -> toléré
                if not files and regulator == "bceao":
                    missing.append(f"{regulator}/{cc}")
        print(f"fichiers de déclaration : {found}")
        if missing:
            raise ValueError(f"Déclarations manquantes pour {report_date} : {missing}. "
                             "Vérifier les logs de build_regulatory_report (lignes écrites) "
                             "et que la couche Silver est alimentée.")
        return found

    countries >> regulatory_report >> check_exports(countries, REPORT_DATE)
