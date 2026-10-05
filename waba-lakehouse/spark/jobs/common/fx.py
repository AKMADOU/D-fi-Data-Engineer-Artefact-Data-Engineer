"""Référentiel de change -> EUR (table silver.fx_rates).

* XOF : parité fixe garantie par le Trésor français, 1 EUR = 655,957 XOF.
* GHS : devise flottante. Série mensuelle **fictive** (≈ 13 GHS pour 1 EUR avec une
  dérive lente), cohérente avec le générateur (1 GHS ≈ 50 XOF). En production, cette
  table serait alimentée par un flux de cours de référence (BCEAO / Bank of Ghana).

La conversion se fait au taux du mois de l'opération (jointure currency + mois).
"""
from __future__ import annotations

import math
from datetime import date

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

XOF_PER_EUR = 655.957
FX_START, FX_END = date(2020, 1, 1), date(2030, 12, 1)


def _months(start: date, end: date):
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        yield date(y, m, 1)
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def ghs_per_eur(month: date) -> float:
    """Série fictive déterministe : tendance + légère saisonnalité."""
    t = (month.year - 2020) * 12 + (month.month - 1)
    return round(11.5 + 0.03 * t + 0.25 * math.sin(t / 3), 4)


def fx_rates_rows() -> list[tuple[str, date, float, str]]:
    rows = []
    for month in _months(FX_START, FX_END):
        rows.append(("XOF", month, round(1 / XOF_PER_EUR, 10), "BCEAO_FIXED_PEG"))
        rows.append(("GHS", month, round(1 / ghs_per_eur(month), 10), "SYNTHETIC_REFERENCE"))
    return rows


def fx_rates_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        fx_rates_rows(), "currency string, rate_month date, eur_per_unit double, source string")


def with_eur(df: DataFrame, fx: DataFrame, ts_col: str, amounts: dict[str, str],
             currency_col: str = "currency") -> DataFrame:
    """Ajoute des colonnes <nom>_eur = montant x taux du mois de `ts_col`."""
    rates = fx.select(F.col("currency").alias("_fx_cur"), F.col("rate_month").alias("_fx_m"),
                      F.col("eur_per_unit").alias("_fx_rate"))
    joined = df.join(
        F.broadcast(rates),
        (F.col(currency_col) == F.col("_fx_cur"))
        & (F.col("_fx_m") == F.trunc(F.col(ts_col).cast("date"), "month")),
        "left")
    for src, dst in amounts.items():
        joined = joined.withColumn(dst, eur(F.col(src), F.col("_fx_rate")))
    return joined.drop("_fx_cur", "_fx_m", "_fx_rate")


def eur(amount: Column, rate: Column) -> Column:
    return F.round(amount.cast("double") * rate, 2).cast("decimal(20,2)")
