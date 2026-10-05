"""Agrégats réglementaires quotidiens (J+1) pour la BCEAO (banque) et la CIMA (assurance).

Grain : une date de reporting D (la veille de l'exécution) x pays [x produit].
  * BCEAO : activité du jour D (volumes, opérations de montant élevé, sorties
    transfrontalières), stocks à fin de D (dépôts, encours de crédit), ratio NPL.
  * CIMA  : cumul du mois à date (MTD) des primes et sinistres par produit,
    loss ratio MTD et délai moyen de traitement des sinistres.
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .gold_transforms import (LOSS_RATIO_THRESHOLD_CIMA, NPL_THRESHOLD_BCEAO, _ratio, _sum_eur,
                              npl_ratio_by_country)


def bceao_daily(silver: dict[str, DataFrame], report_date: date) -> DataFrame:
    d = F.lit(report_date).cast("date")
    bank = silver["bank_transactions"].filter((F.col("txn_date") == d) & ~F.col("is_aberrant"))
    mm = silver["mobile_money_payments"].filter((F.col("txn_date") == d) & ~F.col("is_aberrant"))
    acc = silver["accounts"]

    activity = bank.groupBy("country_code").agg(
        F.count("*").alias("nb_bank_transactions"),
        _sum_eur(F.when(F.col("is_success"), F.col("amount_eur"))).alias("bank_amount_eur"),
        F.sum((F.col("is_high_value") & F.col("is_success")).cast("int"))
        .alias("nb_high_value_transactions"),
        _sum_eur(F.when(F.col("is_cross_border") & F.col("is_success"), F.col("amount_eur")))
        .alias("bank_cross_border_out_eur"))
    mobile = mm.groupBy("country_code").agg(
        F.count("*").alias("nb_mobile_money_transactions"),
        _sum_eur(F.when(F.col("is_success"), F.col("amount_eur"))).alias("mobile_money_amount_eur"),
        _sum_eur(F.when(F.col("is_cross_border") & F.col("is_success"), F.col("amount_eur")))
        .alias("mm_cross_border_out_eur"))
    stocks = acc.filter(F.col("status") == "ACTIVE").groupBy("country_code").agg(
        _sum_eur(F.when(F.col("account_type").isin("CURRENT", "SAVINGS"), F.col("balance_eur")))
        .alias("total_deposits_eur"),
        _sum_eur(F.col("outstanding_eur")).alias("total_loans_outstanding_eur"))
    npl = (npl_ratio_by_country(silver["loan_repayments"], acc, as_of=report_date)
           .filter(F.col("loan_type") == "ALL")
           .select("country_code", "npl_ratio", "outstanding_npl_eur"))

    countries = (acc.select("country_code").distinct())
    out = (countries.join(activity, "country_code", "left").join(mobile, "country_code", "left")
           .join(stocks, "country_code", "left").join(npl, "country_code", "left"))
    zero_cols = ["nb_bank_transactions", "nb_high_value_transactions",
                 "nb_mobile_money_transactions"]
    out = out.fillna(0, subset=zero_cols)
    for c in ["bank_amount_eur", "bank_cross_border_out_eur", "mobile_money_amount_eur",
              "mm_cross_border_out_eur", "total_deposits_eur", "total_loans_outstanding_eur",
              "outstanding_npl_eur"]:
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit(0)).cast("decimal(20,2)"))
    return (out
            .withColumn("cross_border_outflows_eur",
                        (F.col("bank_cross_border_out_eur") + F.col("mm_cross_border_out_eur"))
                        .cast("decimal(20,2)"))
            .withColumn("npl_threshold", F.lit(NPL_THRESHOLD_BCEAO))
            .withColumn("npl_breach", F.coalesce(F.col("npl_ratio") >= NPL_THRESHOLD_BCEAO,
                                                 F.lit(False)))
            .withColumn("report_date", d)
            .withColumn("regulator", F.lit("BCEAO"))
            .withColumn("generated_at", F.current_timestamp())
            .select("report_date", "regulator", "country_code", "nb_bank_transactions",
                    "bank_amount_eur", "nb_high_value_transactions",
                    "nb_mobile_money_transactions", "mobile_money_amount_eur",
                    "cross_border_outflows_eur", "total_deposits_eur",
                    "total_loans_outstanding_eur", "outstanding_npl_eur", "npl_ratio",
                    "npl_threshold", "npl_breach", "generated_at"))


def cima_daily(silver: dict[str, DataFrame], report_date: date) -> DataFrame:
    d = F.lit(report_date).cast("date")
    ops = silver["insurance_operations"].filter(
        (F.col("op_date") <= d) & (F.col("op_date") >= F.trunc(d, "month")) & ~F.col("is_aberrant"))
    return (ops.groupBy("country_code", "line_family", "product_line")
            .agg(_sum_eur(F.when(F.col("is_premium"), F.col("amount_eur"))).alias("premiums_mtd_eur"),
                 _sum_eur(F.when(F.col("is_claim_paid"), F.col("amount_eur")))
                 .alias("claims_paid_mtd_eur"),
                 F.sum((F.col("claim_status") == "PENDING").cast("int")).alias("nb_claims_pending"),
                 F.round(F.avg("processing_days"), 1).alias("avg_processing_days"))
            .withColumn("loss_ratio_mtd", _ratio(F.col("claims_paid_mtd_eur"),
                                                 F.col("premiums_mtd_eur")))
            .withColumn("loss_ratio_threshold", F.lit(LOSS_RATIO_THRESHOLD_CIMA))
            .withColumn("loss_ratio_alert", F.coalesce(
                F.col("loss_ratio_mtd") > LOSS_RATIO_THRESHOLD_CIMA, F.lit(False)))
            .withColumn("report_date", d)
            .withColumn("regulator", F.lit("CIMA"))
            .withColumn("generated_at", F.current_timestamp())
            .select("report_date", "regulator", "country_code", "line_family", "product_line",
                    "premiums_mtd_eur", "claims_paid_mtd_eur", "loss_ratio_mtd",
                    "loss_ratio_threshold", "loss_ratio_alert", "nb_claims_pending",
                    "avg_processing_days", "generated_at"))
