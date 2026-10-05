"""Crée les namespaces (raw, bronze, silver, gold, audit) et les tables d'ingestion. Idempotent.

    spark-submit jobs/create_tables.py            # crée ce qui manque
    spark-submit jobs/create_tables.py --print    # affiche le DDL sans l'exécuter
"""
from __future__ import annotations

import argparse
import sys

from common.lakehouse import AUDIT_DDL, create_table_sql
from common.schemas import DATASETS


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--print", action="store_true", help="afficher le DDL uniquement")
    parser.add_argument("--namespace", default="bronze", choices=["bronze", "raw"])
    args = parser.parse_args()
    if args.print:
        for spec in DATASETS.values():
            print(create_table_sql(spec, args.namespace) + ";\n")
        for ddl in AUDIT_DDL:
            print(ddl + ";\n")
        return 0

    from common.lakehouse import ensure_tables
    from common.session import build_spark

    spark = build_spark("waba-create-tables")
    ensure_tables(spark, args.namespace)
    spark.sql(f"SHOW TABLES IN {args.namespace}").show(truncate=False)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
