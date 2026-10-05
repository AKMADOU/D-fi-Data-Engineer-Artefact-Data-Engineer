"""Job Silver -> Gold : calcul des 7 tables de KPIs (partitionnées par pays).

    spark-submit jobs/gold.py                                    # 7 tables, 8 pays
    spark-submit jobs/gold.py --domain insurance                 # loss ratio + délais sinistres
    spark-submit jobs/gold.py --tables npl_ratio_by_country --countries CI

Idempotent : seules les partitions des pays demandés sont remplacées.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import date

from common.gold_transforms import DOMAINS, GOLD_TABLES, build_gold
from common.jobutils import add_country_arg, j, read_layer, setup_logging, validate_countries
from common.lakehouse import ensure_namespaces, overwrite_partitions
from common.session import build_spark
from common.silver_transforms import EVENT_TABLES

log = setup_logging("gold")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    add_country_arg(p)
    g = p.add_mutually_exclusive_group()
    g.add_argument("--tables", nargs="+", choices=list(GOLD_TABLES))
    g.add_argument("--domain", choices=DOMAINS)
    p.add_argument("--as-of", type=date.fromisoformat,
                   help="Date d'arrêté pour les stocks (NPL). Défaut : toutes les données")
    args = p.parse_args(argv)
    countries = validate_countries(args.countries)
    tables = args.tables or [t for t, (d, _) in GOLD_TABLES.items()
                             if args.domain in (None, d)]

    t0 = time.time()
    spark = build_spark(f"waba-gold-{args.domain or 'all'}")
    ensure_namespaces(spark)
    silver = read_layer(spark, "silver", EVENT_TABLES, countries)
    # Référentiels : non filtrés (lookups), le filtre pays s'applique aux faits.
    silver.update(read_layer(spark, "silver", ["customers", "accounts"]))

    results = {}
    for name, df in build_gold(silver, tables, args.as_of).items():
        df = df.filter(df.country_code.isin(*countries))
        results[name] = overwrite_partitions(spark, df, f"gold.{name}", ("country_code",),
                                             GOLD_TABLES[name][1])
        if results[name] == 0:
            log.warning(j(table=f"gold.{name}", warning="table vide pour ces pays"))

    log.info(j(status="SUCCESS", layer="gold", countries=countries, rows=results,
               duration_s=round(time.time() - t0, 1)))
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
