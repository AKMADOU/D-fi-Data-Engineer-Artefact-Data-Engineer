"""Utilitaires communs aux jobs Spark (logging JSON, arguments pays, lecture des couches)."""
from __future__ import annotations

import argparse
import json
import logging
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .schemas import COUNTRIES


def setup_logging(name: str) -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO, stream=sys.stdout,
        format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":%(message)s}')
    return logging.getLogger(name)


def j(**kw) -> str:
    return json.dumps(kw, default=str)


def add_country_arg(p: argparse.ArgumentParser) -> None:
    p.add_argument("--countries", nargs="+", default=list(COUNTRIES), type=str.upper,
                   help="Pays à (re)calculer ; les autres partitions ne sont pas touchées")


def validate_countries(countries: list[str]) -> list[str]:
    unknown = sorted(set(countries) - set(COUNTRIES))
    if unknown:
        raise SystemExit(f"Pays inconnus : {unknown} (attendus : {COUNTRIES})")
    return countries


def read_layer(spark: SparkSession, namespace: str, names, countries: list[str] | None = None,
               country_col: str = "country_code") -> dict[str, DataFrame]:
    out = {}
    for name in names:
        df = spark.table(f"{namespace}.{name}")
        if countries:
            df = df.filter(F.col(country_col).isin(*countries))
        out[name] = df
    return out


def assert_unique(spark: SparkSession, table: str, key: str, countries: list[str]) -> int:
    """Contrôle qualité : aucune clé en double dans la table écrite (pour ces pays)."""
    row = spark.sql(
        f"SELECT count(*) - count(DISTINCT `{key}`) AS d FROM {table} "
        f"WHERE country_code IN ({', '.join(repr(c) for c in countries)})").collect()[0]
    if row["d"]:
        raise RuntimeError(f"{row['d']} doublon(s) de {key} dans {table}")
    return 0
