"""Job Bronze -> Silver : nettoyage, dédoublonnage, enrichissement, EUR, pseudonymisation.

    spark-submit jobs/silver.py                                  # tous les pays, toutes les tables
    spark-submit jobs/silver.py --countries CI SN                # seulement ces partitions
    spark-submit jobs/silver.py --tables customers accounts      # sous-ensemble de tables

Idempotent : les partitions (pays [, mois]) concernées sont réécrites à l'identique
(dynamic partition overwrite), la quarantaine est purgée puis réécrite pour ces pays.
"""
from __future__ import annotations

import argparse
import os
import sys
import time

from pyspark.sql import functions as F

from common.fx import fx_rates_df
from common.jobutils import add_country_arg, assert_unique, j, setup_logging, validate_countries
from common.lakehouse import QUARANTINE_TABLE, ensure_audit_tables, overwrite_partitions
from common.session import build_spark
from common.silver_transforms import (EVENT_TABLES, REFERENTIAL_TABLES, SILVER_PARTITIONING,
                                      build_silver)

log = setup_logging("silver")

KEYS = {"customers": "customer_key", "accounts": "account_key", "branches": "branch_id",
        "products": "product_id", "bank_transactions": "transaction_id",
        "insurance_operations": "operation_id", "mobile_money_payments": "payment_id",
        "loan_repayments": "repayment_id"}
COMMENTS = {
    "customers": "Clients nettoyés et pseudonymisés (customer_key = SHA-256 salé)",
    "accounts": "Comptes enrichis (client, produit), soldes et encours en EUR",
    "branches": "Agences nettoyées", "products": "Catalogue produits nettoyé",
    "bank_transactions": "Transactions bancaires dédupliquées, enrichies, converties en EUR",
    "insurance_operations": "Opérations d'assurance enrichies, famille IARD/VIE, montants EUR",
    "mobile_money_payments": "Paiements mobile money enrichis, corridors, montants EUR",
    "loan_repayments": "Remboursements enrichis : encours, intérêts perçus, créances douteuses",
}


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    add_country_arg(p)
    p.add_argument("--tables", nargs="+", default=list(REFERENTIAL_TABLES + EVENT_TABLES),
                   choices=list(REFERENTIAL_TABLES + EVENT_TABLES))
    args = p.parse_args(argv)
    countries = validate_countries(args.countries)
    secret = os.environ.get("PII_HASH_SECRET", "")

    t0 = time.time()
    spark = build_spark(f"waba-silver-{'-'.join(args.tables) if len(args.tables) < 3 else 'all'}")
    ensure_audit_tables(spark)

    # Référentiel de change (petite table, réécrite entièrement : idempotent)
    fx = fx_rates_df(spark)
    fx.writeTo("silver.fx_rates").using("iceberg").createOrReplace()
    fx = spark.table("silver.fx_rates").cache()

    bronze = {n: spark.table(f"bronze.{n}") for n in REFERENTIAL_TABLES + EVENT_TABLES}
    result = build_silver(bronze, fx, secret, countries, tuple(args.tables))

    written = {}
    for name, df in result.tables.items():
        table = f"silver.{name}"
        written[name] = overwrite_partitions(spark, df, table, SILVER_PARTITIONING[name],
                                             COMMENTS[name])
        assert_unique(spark, table, KEYS[name], countries)

    if result.quarantine is not None:
        q = result.quarantine.cache()
        n_q = q.count()
        # Purge puis réécriture pour les datasets/pays traités : rejouer ne duplique rien.
        spark.sql(f"DELETE FROM {QUARANTINE_TABLE} WHERE dataset IN "
                  f"({', '.join(repr(t) for t in args.tables)}) AND country_code IN "
                  f"({', '.join(repr(c) for c in countries)})")
        if n_q:
            q.writeTo(QUARANTINE_TABLE).append()
        by_reason = {f"{r['dataset']}:{r['reason']}": r["count"]
                     for r in q.groupBy("dataset", "reason").count().collect()}
    else:
        n_q, by_reason = 0, {}

    log.info(j(status="SUCCESS", layer="silver", countries=countries, rows=written,
               quarantined=n_q, quarantine_reasons=by_reason,
               duration_s=round(time.time() - t0, 1)))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
