"""Reporting réglementaire J+1 (BCEAO / CIMA) pour une date de reporting D.

    spark-submit jobs/regulatory_report.py --report-date 2026-06-15
    spark-submit jobs/regulatory_report.py                        # D = veille (UTC)

Sorties :
  * gold.regulatory_bceao_daily  (report_date, pays)
  * gold.regulatory_cima_daily   (report_date, pays, produit)
  * fichiers CSV de déclaration : s3://regulatory-reports/<bceao|cima>/report_date=D/country_code=XX/
Idempotent : relancer pour la même date remplace les partitions (date, pays) et les fichiers.
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta, timezone

from common.jobutils import add_country_arg, j, read_layer, setup_logging, validate_countries
from common.lakehouse import ensure_namespaces, overwrite_partitions
from common.regulatory import bceao_daily, cima_daily
from common.session import build_spark
from common.silver_transforms import EVENT_TABLES

log = setup_logging("regulatory")


def main(argv: list[str] | None = None) -> int:
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    add_country_arg(p)
    p.add_argument("--report-date", type=date.fromisoformat, default=yesterday)
    p.add_argument("--no-export", action="store_true", help="ne pas produire les CSV")
    args = p.parse_args(argv)
    countries = validate_countries(args.countries)
    d = args.report_date

    t0 = time.time()
    spark = build_spark(f"waba-regulatory-{d}")
    ensure_namespaces(spark)
    silver = read_layer(spark, "silver", EVENT_TABLES + ("accounts",), countries)
    if silver["accounts"].limit(1).count() == 0:
        # Sans référentiel Silver, aucune déclaration ne peut être produite : échec explicite
        # plutôt qu'un succès silencieux avec des fichiers vides.
        raise SystemExit("silver.accounts est vide pour ces pays : exécuter d'abord "
                         "dag_ingest_raw -> dag_bronze_to_silver (make airflow-pipeline)")

    reports = {
        "regulatory_bceao_daily": ("bceao", bceao_daily(silver, d),
                                   "Déclaration quotidienne BCEAO (activité, dépôts, crédits, NPL)"),
        "regulatory_cima_daily": ("cima", cima_daily(silver, d),
                                  "Déclaration quotidienne CIMA (primes, sinistres, loss ratio MTD)"),
    }
    bucket = os.environ.get("REPORTS_BUCKET", "regulatory-reports")
    rows = {}
    for table, (regulator, df, comment) in reports.items():
        df = df.cache()
        rows[table] = overwrite_partitions(spark, df, f"gold.{table}",
                                           ("report_date", "country_code"), comment)
        if not args.no_export:
            path = f"s3a://{bucket}/{regulator}/report_date={d.isoformat()}"
            # Écrasement dynamique : seuls les dossiers des pays recalculés sont remplacés.
            (df.drop("report_date").repartition("country_code").write
             .mode("overwrite").option("header", "true")
             .option("partitionOverwriteMode", "dynamic")
             .partitionBy("country_code").csv(path))
            log.info(j(exported=path))

    log.info(j(status="SUCCESS", layer="regulatory", report_date=d, countries=countries,
               rows=rows, duration_s=round(time.time() - t0, 1)))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
