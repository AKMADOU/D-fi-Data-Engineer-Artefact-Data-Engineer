"""Transformations Bronze -> Silver (fonctions pures sur DataFrames, testables sans Iceberg).

Pour chaque dataset :
  * déduplication sur la clé métier (dernière version ingérée) ;
  * normalisation : codes en majuscules, espaces supprimés, valeurs nulles remplacées
    par des valeurs par défaut explicites (`UNKNOWN`, 0) ;
  * jointure avec les référentiels (customers, accounts, branches, products) ;
  * conversion des montants en EUR (taux du mois de l'opération) ;
  * pseudonymisation : les identifiants client/compte sont remplacés par un hash
    SHA-256 salé (`*_key`). Le lien avec l'identifiant réel n'existe qu'en Bronze,
    zone à accès restreint ;
  * contrôle d'intégrité : les lignes dont une clé étrangère est absente des
    référentiels partent en quarantaine (audit.silver_quarantine) ;
  * cas aberrants : signalés (`is_aberrant`) et exclus des KPIs Gold, pas supprimés.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from .fx import XOF_PER_EUR, eur, with_eur
from .schemas import UEMOA

# Seuil de déclaration des opérations importantes : 10 M XOF (≈ 15 245 EUR).
HIGH_VALUE_EUR = round(10_000_000 / XOF_PER_EUR, 2)
# Au-delà d'1 M EUR pour une opération de détail, la ligne est jugée aberrante.
ABERRANT_EUR = 1_000_000


@dataclass
class SilverResult:
    tables: dict[str, DataFrame]
    quarantine: DataFrame


# ---------------------------------------------------------------------------
# Utilitaires
# ---------------------------------------------------------------------------
def pseudo(col: Column, secret: str) -> Column:
    """Pseudonymisation déterministe (HMAC-like : SHA-256 d'un secret + identifiant).

    Déterministe -> les jointures restent possibles entre tables Silver/Gold ;
    irréversible sans le secret (PII_HASH_SECRET, fourni par l'environnement)."""
    return F.when(col.isNull(), F.lit(None)).otherwise(
        F.sha2(F.concat_ws("|", F.lit(secret), col), 256))


def latest(df: DataFrame, key: str) -> DataFrame:
    """Déduplication : une ligne par clé, la plus récemment ingérée."""
    w = Window.partitionBy(key).orderBy(F.col("_ingested_at").desc_nulls_last(),
                                        F.col("_source_file").desc_nulls_last())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def clean_code(col: str) -> Column:
    return F.upper(F.trim(F.col(col)))


def _quarantine(df: DataFrame, dataset: str, key: str) -> DataFrame:
    return df.select(F.lit(dataset).alias("dataset"), F.col(key).alias("record_key"),
                     F.col("country_code"), F.col("_dq_reason").alias("reason"),
                     F.current_timestamp().alias("detected_at"))


def _split_orphans(df: DataFrame, checks: list[tuple[Column, str]]) -> tuple[DataFrame, DataFrame]:
    reason = F.concat_ws(";", *[F.when(cond, F.lit(label)) for cond, label in checks])
    flagged = df.withColumn("_dq_reason", reason)
    return flagged.filter("_dq_reason = ''").drop("_dq_reason"), flagged.filter("_dq_reason != ''")


def _aberrant(amount_eur: Column, ts: Column) -> Column:
    return ((amount_eur > ABERRANT_EUR) | (amount_eur <= 0)
            | (ts > F.current_timestamp() + F.expr("INTERVAL 1 DAY")))


# ---------------------------------------------------------------------------
# Référentiels
# ---------------------------------------------------------------------------
def silver_customers(customers: DataFrame, secret: str) -> DataFrame:
    c = latest(customers, "customer_id")
    return c.select(
        pseudo(F.col("customer_id"), secret).alias("customer_key"),
        clean_code("country_code").alias("country_code"),
        clean_code("entity_type").alias("entity_type"),
        F.coalesce(clean_code("segment"), F.lit("UNKNOWN")).alias("segment"),
        F.coalesce(clean_code("kyc_level"), F.lit("UNKNOWN")).alias("kyc_level"),
        F.col("onboarding_date"),
        F.coalesce(F.trim(F.col("region")), F.lit("UNKNOWN")).alias("region"),
        F.coalesce(F.col("is_active"), F.lit(False)).alias("is_active"),
        F.datediff(F.current_date(), F.col("onboarding_date")).alias("tenure_days"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.current_timestamp().alias("_silver_processed_at"))


def silver_branches(branches: DataFrame) -> DataFrame:
    b = latest(branches, "branch_id")
    return b.select(
        F.col("branch_id"), clean_code("country_code").alias("country_code"),
        clean_code("entity_type").alias("entity_type"),
        F.coalesce(F.trim(F.col("city")), F.lit("UNKNOWN")).alias("city"),
        F.coalesce(F.trim(F.col("region")), F.lit("UNKNOWN")).alias("region"),
        F.coalesce(clean_code("branch_type"), F.lit("UNKNOWN")).alias("branch_type"),
        F.coalesce(F.col("is_active"), F.lit(False)).alias("is_active"),
        F.current_timestamp().alias("_silver_processed_at"))


def silver_products(products: DataFrame, fx: DataFrame) -> DataFrame:
    p = latest(products, "product_id")
    p = p.withColumn("_now", F.current_date())
    p = with_eur(p, fx, "_now", {"monthly_fee": "monthly_fee_eur"})
    return p.select(
        "product_id", F.trim(F.col("product_name")).alias("product_name"),
        clean_code("product_category").alias("product_category"),
        clean_code("entity_type").alias("entity_type"),
        clean_code("country_code").alias("country_code"), "currency",
        F.coalesce(F.col("interest_rate"), F.lit(0)).cast("decimal(6,2)").alias("interest_rate"),
        F.coalesce(F.col("monthly_fee"), F.lit(0)).alias("monthly_fee"), "monthly_fee_eur",
        "launch_date", F.coalesce(F.col("is_active"), F.lit(False)).alias("is_active"),
        F.current_timestamp().alias("_silver_processed_at"))


def account_lookup(accounts: DataFrame) -> DataFrame:
    """Vue interne (non persistée) : identifiants réels -> attributs du compte."""
    return latest(accounts, "account_id").select(
        "account_id", "customer_id", F.col("country_code").alias("acc_country"),
        F.col("entity_type").alias("acc_entity"), "account_type", "product_id",
        "balance", "currency", F.col("status").alias("account_status"))


def silver_accounts(accounts: DataFrame, customers: DataFrame, products: DataFrame,
                    fx: DataFrame, secret: str) -> tuple[DataFrame, DataFrame]:
    a = latest(accounts, "account_id")
    cust = latest(customers, "customer_id").select(
        "customer_id", F.col("segment").alias("customer_segment"), "kyc_level")
    prod = latest(products, "product_id").select("product_id", "product_name", "interest_rate")
    df = (a.join(cust, "customer_id", "left")
          .join(prod, "product_id", "left")
          .withColumn("_customer_found", F.col("customer_segment").isNotNull()))
    # Soldes : valorisés au taux du mois courant (photo « as of » du référentiel).
    df = df.withColumn("_now", F.current_date())
    df = with_eur(df, fx, "_now", {"balance": "balance_eur", "credit_limit": "credit_limit_eur"})
    ok, bad = _split_orphans(df, [(~F.col("_customer_found"), "ORPHAN_CUSTOMER")])
    silver = ok.select(
        pseudo(F.col("account_id"), secret).alias("account_key"),
        pseudo(F.col("customer_id"), secret).alias("customer_key"),
        clean_code("country_code").alias("country_code"),
        clean_code("entity_type").alias("entity_type"),
        clean_code("account_type").alias("account_type"),
        "product_id", F.coalesce(F.col("product_name"), F.lit("UNKNOWN")).alias("product_name"),
        F.coalesce(F.col("interest_rate"), F.lit(0)).cast("decimal(6,2)").alias("interest_rate"),
        "currency", "balance", F.coalesce(F.col("credit_limit"), F.lit(0)).alias("credit_limit"),
        "balance_eur", F.coalesce(F.col("credit_limit_eur"), F.lit(0)).alias("credit_limit_eur"),
        # Encours de crédit = solde débiteur d'un compte LOAN (valeur positive)
        F.when(F.col("account_type") == "LOAN", -F.col("balance_eur"))
         .otherwise(F.lit(0)).cast("decimal(20,2)").alias("outstanding_eur"),
        "opened_date", clean_code("status").alias("status"),
        F.coalesce(F.col("customer_segment"), F.lit("UNKNOWN")).alias("customer_segment"),
        F.coalesce(F.col("kyc_level"), F.lit("UNKNOWN")).alias("kyc_level"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.current_timestamp().alias("_silver_processed_at"))
    return silver, _quarantine(bad.withColumn("country_code", F.col("country_code")),
                               "accounts", "account_id")


# ---------------------------------------------------------------------------
# Événements
# ---------------------------------------------------------------------------
def silver_bank_transactions(txn: DataFrame, accounts: DataFrame, customers: DataFrame,
                             branches: DataFrame, fx: DataFrame,
                             secret: str) -> tuple[DataFrame, DataFrame]:
    t = latest(txn, "transaction_id")
    acc = account_lookup(accounts)
    ben = acc.select(F.col("account_id").alias("_ben_id"),
                     F.col("acc_country").alias("beneficiary_country"))
    cust = latest(customers, "customer_id").select(
        "customer_id", F.col("segment").alias("customer_segment"))
    br = latest(branches, "branch_id").select(
        "branch_id", F.col("city").alias("branch_city"), F.col("region").alias("branch_region"),
        "branch_type", F.lit(True).alias("_branch_found"))
    df = (t.join(acc.select("account_id", "customer_id", "account_type"), "account_id", "left")
          .join(ben, F.col("beneficiary_account") == F.col("_ben_id"), "left")
          .join(br, "branch_id", "left")
          .join(cust, "customer_id", "left"))
    df, bad = _split_orphans(df, [
        (F.col("account_type").isNull(), "ORPHAN_ACCOUNT"),
        (F.col("beneficiary_account").isNotNull() & F.col("_ben_id").isNull(),
         "ORPHAN_BENEFICIARY"),
        (F.col("_branch_found").isNull(), "ORPHAN_BRANCH"),
    ])
    df = with_eur(df, fx, "timestamp", {"amount": "amount_eur", "fee_amount": "fee_amount_eur"})
    silver = df.select(
        "transaction_id", "timestamp", F.to_date("timestamp").alias("txn_date"),
        clean_code("country_code").alias("country_code"),
        clean_code("entity_type").alias("entity_type"),
        clean_code("transaction_type").alias("transaction_type"),
        F.coalesce(clean_code("channel"), F.lit("UNKNOWN")).alias("channel"),
        clean_code("transaction_status").alias("transaction_status"),
        (F.col("transaction_status") == "SUCCESS").alias("is_success"),
        "amount", F.coalesce(F.col("fee_amount"), F.lit(0)).alias("fee_amount"), "currency",
        "amount_eur", F.coalesce(F.col("fee_amount_eur"), F.lit(0)).alias("fee_amount_eur"),
        pseudo(F.col("account_id"), secret).alias("account_key"),
        pseudo(F.col("customer_id"), secret).alias("customer_key"),
        pseudo(F.col("beneficiary_account"), secret).alias("beneficiary_account_key"),
        "beneficiary_country", "account_type",
        F.coalesce(F.col("customer_segment"), F.lit("UNKNOWN")).alias("customer_segment"),
        "branch_id", "branch_city", "branch_region", "branch_type",
        ((F.col("transaction_type") == "INTERNATIONAL_WIRE")
         | (F.col("beneficiary_country") != F.col("country_code"))).alias("is_cross_border"),
        (F.col("amount_eur") >= HIGH_VALUE_EUR).alias("is_high_value"),
        _aberrant(F.col("amount_eur"), F.col("timestamp")).alias("is_aberrant"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.current_timestamp().alias("_silver_processed_at"))
    return silver, _quarantine(bad, "bank_transactions", "transaction_id")


def silver_insurance_operations(ops: DataFrame, accounts: DataFrame, customers: DataFrame,
                                fx: DataFrame, secret: str) -> tuple[DataFrame, DataFrame]:
    o = latest(ops, "operation_id")
    acc = account_lookup(accounts).select(
        F.col("account_id").alias("_acc_id"), F.col("customer_id").alias("_acc_owner"),
        F.col("account_type").alias("_acc_type"))
    cust = latest(customers, "customer_id").select(
        "customer_id", F.col("segment").alias("customer_segment"))
    df = (o.join(acc, F.col("account_id") == F.col("_acc_id"), "left")
          .join(cust, "customer_id", "left"))
    df, bad = _split_orphans(df, [
        (F.col("_acc_id").isNull(), "ORPHAN_POLICY_ACCOUNT"),
        (F.col("customer_segment").isNull(), "ORPHAN_CUSTOMER"),
        (F.col("_acc_owner").isNotNull() & (F.col("_acc_owner") != F.col("customer_id")),
         "POLICY_OWNER_MISMATCH"),
    ])
    df = with_eur(df, fx, "timestamp", {"amount": "amount_eur"})
    op = F.col("operation_type")
    is_claim = op.isin("CLAIM_SUBMISSION", "CLAIM_PAYMENT")
    silver = df.select(
        "operation_id", "timestamp", F.to_date("timestamp").alias("op_date"),
        F.trunc(F.to_date("timestamp"), "month").alias("op_month"),
        clean_code("country_code").alias("country_code"),
        clean_code("entity_type").alias("entity_type"),
        clean_code("operation_type").alias("operation_type"),
        clean_code("product_line").alias("product_line"),
        F.when(F.col("product_line").isin("VIE", "PREVOYANCE"), F.lit("VIE"))
         .otherwise(F.lit("IARD")).alias("line_family"),
        "amount", "currency", "amount_eur",
        # Statut et délai n'ont de sens que pour un sinistre : NULL sinon (normalisation).
        F.when(is_claim, clean_code("claim_status")).alias("claim_status"),
        F.when(is_claim, F.col("processing_days")).alias("processing_days"),
        op.isin("PREMIUM_PAYMENT", "POLICY_RENEWAL").alias("is_premium"),
        is_claim.alias("is_claim"), (op == "CLAIM_PAYMENT").alias("is_claim_paid"),
        pseudo(F.col("customer_id"), secret).alias("customer_key"),
        pseudo(F.col("account_id"), secret).alias("account_key"),
        "customer_segment",
        _aberrant(F.col("amount_eur"), F.col("timestamp")).alias("is_aberrant"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.current_timestamp().alias("_silver_processed_at"))
    return silver, _quarantine(bad, "insurance_operations", "operation_id")


def silver_mobile_money(payments: DataFrame, customers: DataFrame, fx: DataFrame,
                        secret: str) -> tuple[DataFrame, DataFrame]:
    p = latest(payments, "payment_id")
    cust = latest(customers, "customer_id").select(
        "customer_id", F.col("segment").alias("customer_segment"),
        F.col("country_code").alias("sender_home_country"))
    rcv = cust.select(F.col("customer_id").alias("_rcv_id"))
    df = (p.join(cust, F.col("sender_id") == F.col("customer_id"), "left")
          .join(rcv, F.col("receiver_id") == F.col("_rcv_id"), "left"))
    df, bad = _split_orphans(df, [
        (F.col("customer_id").isNull(), "ORPHAN_SENDER"),
        (F.col("_rcv_id").isNull(), "ORPHAN_RECEIVER"),
    ])
    df = with_eur(df, fx, "timestamp", {"amount": "amount_eur", "fee_amount": "fee_amount_eur"})
    s, r = clean_code("sender_country"), clean_code("receiver_country")
    silver = df.select(
        "payment_id", "timestamp", F.to_date("timestamp").alias("txn_date"),
        s.alias("country_code"), s.alias("sender_country"), r.alias("receiver_country"),
        # Pays de résidence du client émetteur (référentiel) : sert à détecter les
        # paiements émis depuis un pays inhabituel (Level 3, fraude temps réel).
        "sender_home_country",
        clean_code("entity_type").alias("entity_type"),
        clean_code("payment_type").alias("payment_type"),
        F.coalesce(clean_code("operator"), F.lit("UNKNOWN")).alias("operator"),
        clean_code("status").alias("status"), (F.col("status") == "SUCCESS").alias("is_success"),
        "amount", F.coalesce(F.col("fee_amount"), F.lit(0)).alias("fee_amount"), "currency",
        "amount_eur", F.coalesce(F.col("fee_amount_eur"), F.lit(0)).alias("fee_amount_eur"),
        pseudo(F.col("sender_id"), secret).alias("sender_key"),
        pseudo(F.col("receiver_id"), secret).alias("receiver_key"),
        F.coalesce(F.col("customer_segment"), F.lit("UNKNOWN")).alias("customer_segment"),
        (s != r).alias("is_cross_border"), F.concat_ws("-", s, r).alias("corridor"),
        (s.isin(*UEMOA) & r.isin(*UEMOA)).alias("is_uemoa_corridor"),
        _aberrant(F.col("amount_eur"), F.col("timestamp")).alias("is_aberrant"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.current_timestamp().alias("_silver_processed_at"))
    return silver, _quarantine(bad.withColumn("country_code", F.col("sender_country")),
                               "mobile_money_payments", "payment_id")


def silver_loan_repayments(rep: DataFrame, accounts: DataFrame, customers: DataFrame,
                           products: DataFrame, fx: DataFrame,
                           secret: str) -> tuple[DataFrame, DataFrame]:
    r = latest(rep, "repayment_id")
    acc = account_lookup(accounts).select(
        F.col("account_id").alias("_loan_id"), F.col("customer_id").alias("_owner"),
        F.col("account_type").alias("_acc_type"), "product_id",
        F.col("balance").alias("_balance"))
    prod = latest(products, "product_id").select("product_id", "interest_rate")
    cust = latest(customers, "customer_id").select(
        "customer_id", F.col("segment").alias("customer_segment"))
    df = (r.join(acc, F.col("loan_account_id") == F.col("_loan_id"), "left")
          .join(prod, "product_id", "left")
          .join(cust, "customer_id", "left"))
    df, bad = _split_orphans(df, [
        (F.col("_loan_id").isNull(), "ORPHAN_LOAN_ACCOUNT"),
        (F.col("_acc_type").isNotNull() & (F.col("_acc_type") != "LOAN"), "NOT_A_LOAN_ACCOUNT"),
        (F.col("customer_segment").isNull(), "ORPHAN_CUSTOMER"),
    ])
    df = with_eur(df, fx, "timestamp", {"amount_due": "amount_due_eur",
                                        "amount_paid": "amount_paid_eur",
                                        "_balance": "_balance_eur"})
    outstanding = F.greatest(-F.col("_balance_eur"), F.lit(0))
    rate = F.coalesce(F.col("interest_rate"), F.lit(0)).cast("double")
    # Intérêts perçus : quote-part d'intérêts de l'échéance payée (encours x taux / 12),
    # plafonnée au montant effectivement payé.
    interest = F.least(F.col("amount_paid_eur").cast("double"),
                       outstanding.cast("double") * rate / 100 / 12)
    status = clean_code("repayment_status")
    silver = df.select(
        "repayment_id", "timestamp", F.trunc(F.to_date("timestamp"), "month").alias("event_month"),
        clean_code("country_code").alias("country_code"),
        clean_code("entity_type").alias("entity_type"),
        clean_code("loan_type").alias("loan_type"), status.alias("repayment_status"),
        "due_date", "payment_date", F.coalesce(F.col("days_overdue"), F.lit(0)).alias("days_overdue"),
        "amount_due", "amount_paid", "currency", "amount_due_eur", "amount_paid_eur",
        outstanding.cast("decimal(20,2)").alias("outstanding_eur"),
        F.coalesce(F.col("interest_rate"), F.lit(0)).cast("decimal(6,2)").alias("interest_rate"),
        eur(interest, F.lit(1.0)).alias("interest_received_eur"),
        # Créance douteuse : défaut de paiement ou retard > 90 jours (norme prudentielle)
        ((status == "DEFAULT") | (F.col("days_overdue") > 90)).alias("is_non_performing"),
        pseudo(F.col("loan_account_id"), secret).alias("account_key"),
        pseudo(F.col("customer_id"), secret).alias("customer_key"),
        "customer_segment",
        _aberrant(F.col("amount_due_eur"), F.col("timestamp")).alias("is_aberrant"),
        F.col("_ingested_at").alias("_bronze_ingested_at"),
        F.current_timestamp().alias("_silver_processed_at"))
    return silver, _quarantine(bad, "loan_repayments", "repayment_id")


# ---------------------------------------------------------------------------
# Orchestration des transformations
# ---------------------------------------------------------------------------
SILVER_PARTITIONING: dict[str, tuple[str, ...]] = {
    "customers": ("country_code",),
    "accounts": ("country_code",),
    "branches": ("country_code",),
    "products": ("country_code",),
    "bank_transactions": ("country_code", "months(timestamp)"),
    "insurance_operations": ("country_code", "months(timestamp)"),
    "mobile_money_payments": ("country_code", "months(timestamp)"),
    "loan_repayments": ("country_code", "months(timestamp)"),
}
REFERENTIAL_TABLES = ("customers", "accounts", "branches", "products")
EVENT_TABLES = ("bank_transactions", "insurance_operations", "mobile_money_payments",
                "loan_repayments")


def build_silver(bronze: dict[str, DataFrame], fx: DataFrame, secret: str,
                 countries: list[str] | None = None,
                 tables: tuple[str, ...] = REFERENTIAL_TABLES + EVENT_TABLES) -> SilverResult:
    """Construit les tables Silver demandées pour les pays demandés.

    Les référentiels servent de lookup sur TOUS les pays (un virement international
    référence un compte bénéficiaire étranger) ; seul le résultat est filtré par pays."""
    if not secret:
        raise ValueError("PII_HASH_SECRET manquant : pseudonymisation impossible")
    cust, acc, br, prod = (bronze[n] for n in REFERENTIAL_TABLES)
    out: dict[str, DataFrame] = {}
    quarantine: list[DataFrame] = []

    def keep(name: str, result) -> None:
        df, q = result if isinstance(result, tuple) else (result, None)
        if countries:
            df = df.filter(F.col("country_code").isin(*countries))
            q = q.filter(F.col("country_code").isin(*countries)) if q is not None else None
        out[name] = df
        if q is not None:
            quarantine.append(q)

    builders = {
        "customers": lambda: silver_customers(cust, secret),
        "branches": lambda: silver_branches(br),
        "products": lambda: silver_products(prod, fx),
        "accounts": lambda: silver_accounts(acc, cust, prod, fx, secret),
        "bank_transactions": lambda: silver_bank_transactions(
            bronze["bank_transactions"], acc, cust, br, fx, secret),
        "insurance_operations": lambda: silver_insurance_operations(
            bronze["insurance_operations"], acc, cust, fx, secret),
        "mobile_money_payments": lambda: silver_mobile_money(
            bronze["mobile_money_payments"], cust, fx, secret),
        "loan_repayments": lambda: silver_loan_repayments(
            bronze["loan_repayments"], acc, cust, prod, fx, secret),
    }
    for name in tables:
        keep(name, builders[name]())
    q = None
    for part in quarantine:
        q = part if q is None else q.unionByName(part)
    return SilverResult(out, q)
