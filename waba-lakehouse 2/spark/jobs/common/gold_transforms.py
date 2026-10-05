"""Transformations Silver -> Gold : KPIs financiers et réglementaires (fonctions pures).

Conventions :
  * montants consolidés en EUR (comparaison cross-pays) ; le montant en devise
    locale est conservé quand l'agrégat est mono-devise (grain pays) ;
  * les lignes `is_aberrant` sont exclues des KPIs ;
  * chaque table porte `country_code` (partition) et `_gold_computed_at`.
"""
from __future__ import annotations

from datetime import date

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

NPL_THRESHOLD_BCEAO = 0.05       # NPL < 5 %
LOSS_RATIO_THRESHOLD_CIMA = 0.70  # vigilance si > 70 %


def _ratio(num: Column, den: Column, scale: int = 4) -> Column:
    return F.when(den > 0, F.round(num.cast("double") / den.cast("double"), scale))


def _sum_eur(col: Column) -> Column:
    return F.coalesce(F.sum(col), F.lit(0)).cast("decimal(20,2)")


def _stamp(df: DataFrame) -> DataFrame:
    return df.withColumn("_gold_computed_at", F.current_timestamp())


# ---------------------------------------------------------------------------
# KPIs bancaires
# ---------------------------------------------------------------------------
def daily_transaction_volume(bank: DataFrame, mobile_money: DataFrame) -> DataFrame:
    """Volume et montant par jour, pays, entité et type d'opération (banque + mobile money)."""
    cols = ["txn_date", "country_code", "entity_type", "transaction_type", "currency",
            "is_success", "amount", "amount_eur"]
    b = bank.filter(~F.col("is_aberrant")).select(*cols)
    m = (mobile_money.filter(~F.col("is_aberrant"))
         .withColumnRenamed("payment_type", "transaction_type").select(*cols))
    return _stamp(
        b.unionByName(m)
        # Noms de colonnes alignés sur la requête Lambda de l'énoncé (txn_date, total_amount_eur)
        .groupBy("txn_date", "country_code", "entity_type", "transaction_type", "currency")
        .agg(F.count("*").alias("nb_transactions"),
             F.sum(F.col("is_success").cast("int")).alias("nb_success"),
             _sum_eur(F.col("amount")).alias("total_amount_local"),
             _sum_eur(F.col("amount_eur")).alias("total_amount_eur"),
             _sum_eur(F.when(F.col("is_success"), F.col("amount_eur"))).alias("amount_success_eur"),
             F.round(F.avg("amount_eur"), 2).cast("decimal(20,2)").alias("avg_amount_eur"))
        .withColumn("failure_rate",
                    _ratio(F.col("nb_transactions") - F.col("nb_success"), F.col("nb_transactions"))))


def _loan_book(loans: DataFrame, accounts: DataFrame, as_of: date | None) -> DataFrame:
    """Un prêt = un compte LOAN ayant au moins une échéance observée jusqu'à `as_of`.

    Statut du prêt = dernière échéance connue : il est « non performant » si cette
    échéance est en défaut ou en retard de plus de 90 jours."""
    l = loans.filter(~F.col("is_aberrant"))
    if as_of is not None:
        l = l.filter(F.to_date("timestamp") <= F.lit(as_of))
    w = Window.partitionBy("account_key").orderBy(F.col("timestamp").desc())
    last = (l.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
            .select("account_key", "country_code", "entity_type", "loan_type",
                    "is_non_performing"))
    book = accounts.filter(F.col("account_type") == "LOAN").select("account_key", "outstanding_eur")
    return last.join(book, "account_key", "inner")


def _npl_agg(df: DataFrame) -> list[Column]:
    return [F.count("*").alias("nb_loans"),
            F.sum(F.col("is_non_performing").cast("int")).alias("nb_loans_npl"),
            _sum_eur(F.col("outstanding_eur")).alias("outstanding_total_eur"),
            _sum_eur(F.when(F.col("is_non_performing"), F.col("outstanding_eur")))
            .alias("outstanding_npl_eur")]


def npl_ratio_by_country(loans: DataFrame, accounts: DataFrame,
                         as_of: date | None = None) -> DataFrame:
    """NPL = encours des prêts en défaut / encours total, par pays et type de prêt
    (+ une ligne loan_type = 'ALL' par pays). Seuil BCEAO : 5 %."""
    book = _loan_book(loans, accounts, as_of)
    detail = book.groupBy("country_code", "loan_type").agg(*_npl_agg(book))
    total = (book.groupBy("country_code").agg(*_npl_agg(book))
             .withColumn("loan_type", F.lit("ALL")))
    res = detail.unionByName(total)
    as_of_col = F.lit(as_of) if as_of else F.current_date()
    return _stamp(
        res.withColumn("as_of_date", as_of_col.cast("date"))
        .withColumn("npl_ratio", _ratio(F.col("outstanding_npl_eur"), F.col("outstanding_total_eur")))
        .withColumn("bceao_threshold", F.lit(NPL_THRESHOLD_BCEAO))
        .withColumn("is_above_threshold", F.col("npl_ratio") >= NPL_THRESHOLD_BCEAO)
        .select("as_of_date", "country_code", "loan_type", "nb_loans", "nb_loans_npl",
                "outstanding_total_eur", "outstanding_npl_eur", "npl_ratio",
                "bceao_threshold", "is_above_threshold"))


def customer_arpu_monthly(bank: DataFrame, mobile_money: DataFrame, loans: DataFrame,
                          customers: DataFrame) -> DataFrame:
    """ARPC = (commissions + intérêts perçus) / nombre de clients actifs distincts,
    par mois, pays et segment. Client actif = au moins une opération dans le mois."""
    month = lambda c: F.trunc(F.to_date(c), "month")  # noqa: E731
    fees_bank = bank.filter(~F.col("is_aberrant")).select(
        month("timestamp").alias("month"), "country_code", "customer_key",
        F.when(F.col("is_success"), F.col("fee_amount_eur")).alias("commission_eur"),
        F.lit(None).cast("decimal(20,2)").alias("interest_eur"))
    fees_mm = mobile_money.filter(~F.col("is_aberrant")).select(
        month("timestamp").alias("month"), "country_code",
        F.col("sender_key").alias("customer_key"),
        F.when(F.col("is_success"), F.col("fee_amount_eur")).alias("commission_eur"),
        F.lit(None).cast("decimal(20,2)").alias("interest_eur"))
    interest = loans.filter(~F.col("is_aberrant")).select(
        month("timestamp").alias("month"), "country_code", "customer_key",
        F.lit(None).cast("decimal(20,2)").alias("commission_eur"),
        F.col("interest_received_eur").alias("interest_eur"))
    events = fees_bank.unionByName(fees_mm).unionByName(interest)
    seg = customers.select("customer_key", F.col("segment").alias("customer_segment"))
    df = events.join(seg, "customer_key", "left").fillna({"customer_segment": "UNKNOWN"})
    return _stamp(
        df.groupBy("month", "country_code", "customer_segment")
        .agg(_sum_eur(F.col("commission_eur")).alias("commissions_eur"),
             _sum_eur(F.col("interest_eur")).alias("interest_eur"),
             F.countDistinct("customer_key").alias("active_customers"))
        .withColumn("revenue_eur", (F.col("commissions_eur") + F.col("interest_eur"))
                    .cast("decimal(20,2)"))
        .withColumn("arpc_eur", F.round(F.col("revenue_eur") / F.col("active_customers"), 2)
                    .cast("decimal(20,4)")))


# ---------------------------------------------------------------------------
# KPIs assurance
# ---------------------------------------------------------------------------
def loss_ratio_by_product(ops: DataFrame) -> DataFrame:
    """Sinistres payés / primes acquises par mois, pays et produit. Seuil CIMA : 70 %."""
    o = ops.filter(~F.col("is_aberrant"))
    return _stamp(
        o.groupBy(F.col("op_month").alias("month"), "country_code", "line_family", "product_line")
        .agg(_sum_eur(F.when(F.col("is_premium"), F.col("amount_eur"))).alias("premiums_eur"),
             _sum_eur(F.when(F.col("is_claim_paid"), F.col("amount_eur"))).alias("claims_paid_eur"),
             F.sum(F.col("is_premium").cast("int")).alias("nb_premiums"),
             F.sum(F.col("is_claim_paid").cast("int")).alias("nb_claims_paid"))
        .withColumn("loss_ratio", _ratio(F.col("claims_paid_eur"), F.col("premiums_eur")))
        .withColumn("cima_threshold", F.lit(LOSS_RATIO_THRESHOLD_CIMA))
        .withColumn("is_above_threshold", F.col("loss_ratio") > LOSS_RATIO_THRESHOLD_CIMA))


def claims_processing_time(ops: DataFrame) -> DataFrame:
    """Délai de traitement des sinistres (jours ouvrés) par mois, pays et ligne IARD / VIE."""
    c = ops.filter(F.col("is_claim") & F.col("processing_days").isNotNull())
    return _stamp(
        c.groupBy(F.col("op_month").alias("month"), "country_code", "line_family")
        .agg(F.count("*").alias("nb_claims"),
             F.round(F.avg("processing_days"), 1).alias("avg_processing_days"),
             F.percentile_approx("processing_days", 0.5).alias("median_processing_days"),
             F.percentile_approx("processing_days", 0.9).alias("p90_processing_days"),
             F.sum((F.col("claim_status") == "PENDING").cast("int")).alias("nb_pending"),
             F.sum((F.col("claim_status") == "PAID").cast("int")).alias("nb_paid")))


# ---------------------------------------------------------------------------
# KPIs mobile money
# ---------------------------------------------------------------------------
def mobile_money_daily_flow(mm: DataFrame) -> DataFrame:
    m = mm.filter(~F.col("is_aberrant"))
    return _stamp(
        m.groupBy(F.col("txn_date").alias("transaction_date"), "country_code", "currency")
        .agg(F.count("*").alias("nb_transactions"),
             F.sum(F.col("is_success").cast("int")).alias("nb_success"),
             F.sum((F.col("status") == "FAILED").cast("int")).alias("nb_failed"),
             F.sum((F.col("status") == "PENDING").cast("int")).alias("nb_pending"),
             _sum_eur(F.col("amount")).alias("amount_total_local"),
             _sum_eur(F.col("amount_eur")).alias("amount_total_eur"),
             _sum_eur(F.when(F.col("is_success"), F.col("amount_eur"))).alias("amount_success_eur"),
             _sum_eur(F.when(F.col("is_success"), F.col("fee_amount_eur"))).alias("fees_eur"),
             F.countDistinct("sender_key").alias("active_users"),
             F.sum(F.col("is_cross_border").cast("int")).alias("nb_cross_border"))
        .withColumn("failure_rate", _ratio(F.col("nb_failed"), F.col("nb_transactions"))))


def cross_border_transfers(bank: DataFrame, mm: DataFrame) -> DataFrame:
    """Transferts transfrontaliers réussis par semaine et corridor (émetteur -> destinataire),
    via mobile money et virements internationaux, avec évolution hebdomadaire."""
    week = F.to_date(F.date_trunc("week", F.col("timestamp")))
    b = (bank.filter(F.col("is_cross_border") & F.col("is_success") & ~F.col("is_aberrant")
                     & F.col("beneficiary_country").isNotNull())
         .select(week.alias("week_start"), F.col("country_code").alias("sender_country"),
                 F.col("beneficiary_country").alias("receiver_country"),
                 F.lit("BANK_WIRE").alias("channel"), "amount_eur"))
    m = (mm.filter(F.col("is_cross_border") & F.col("is_success") & ~F.col("is_aberrant"))
         .select(week.alias("week_start"), "sender_country", "receiver_country",
                 F.lit("MOBILE_MONEY").alias("channel"), "amount_eur"))
    agg = (b.unionByName(m)
           .groupBy("week_start", "sender_country", "receiver_country", "channel")
           .agg(F.count("*").alias("nb_transfers"),
                _sum_eur(F.col("amount_eur")).alias("amount_total_eur"),
                F.round(F.avg("amount_eur"), 2).cast("decimal(20,2)").alias("avg_amount_eur")))
    w = Window.partitionBy("sender_country", "receiver_country", "channel").orderBy("week_start")
    uemoa = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ"]
    return _stamp(
        agg.withColumn("prev_week_amount_eur", F.lag("amount_total_eur").over(w))
        .withColumn("wow_change_pct",
                    F.round((F.col("amount_total_eur") - F.col("prev_week_amount_eur"))
                            / F.col("prev_week_amount_eur") * 100, 2))
        .withColumn("corridor", F.concat_ws("-", "sender_country", "receiver_country"))
        .withColumn("is_uemoa_corridor", F.col("sender_country").isin(*uemoa)
                    & F.col("receiver_country").isin(*uemoa))
        .withColumn("country_code", F.col("sender_country")))


# ---------------------------------------------------------------------------
# Registre des tables Gold
# ---------------------------------------------------------------------------
GOLD_TABLES = {
    # nom : (domaine, description)
    "daily_transaction_volume": ("banking", "Volume et montant des transactions par jour, pays, entité et type"),
    "npl_ratio_by_country": ("banking", "Taux de créances douteuses par pays et type de prêt (seuil BCEAO 5 %)"),
    "customer_arpu_monthly": ("banking", "Revenu moyen par client actif, par mois, pays et segment"),
    "loss_ratio_by_product": ("insurance", "Sinistres payés / primes acquises par mois, pays et produit (seuil CIMA 70 %)"),
    "claims_processing_time": ("insurance", "Délai de traitement des sinistres en jours ouvrés, par pays et ligne IARD/VIE"),
    "mobile_money_daily_flow": ("mobile_money", "Flux journalier de paiements mobiles par pays"),
    "cross_border_transfers": ("mobile_money", "Transferts transfrontaliers hebdomadaires par corridor"),
}
DOMAINS = ("banking", "insurance", "mobile_money")


def build_gold(silver: dict[str, DataFrame], tables: list[str],
               as_of: date | None = None) -> dict[str, DataFrame]:
    s = silver
    builders = {
        "daily_transaction_volume": lambda: daily_transaction_volume(
            s["bank_transactions"], s["mobile_money_payments"]),
        "npl_ratio_by_country": lambda: npl_ratio_by_country(
            s["loan_repayments"], s["accounts"], as_of),
        "customer_arpu_monthly": lambda: customer_arpu_monthly(
            s["bank_transactions"], s["mobile_money_payments"], s["loan_repayments"],
            s["customers"]),
        "loss_ratio_by_product": lambda: loss_ratio_by_product(s["insurance_operations"]),
        "claims_processing_time": lambda: claims_processing_time(s["insurance_operations"]),
        "mobile_money_daily_flow": lambda: mobile_money_daily_flow(s["mobile_money_payments"]),
        "cross_border_transfers": lambda: cross_border_transfers(
            s["bank_transactions"], s["mobile_money_payments"]),
    }
    return {t: builders[t]() for t in tables}
