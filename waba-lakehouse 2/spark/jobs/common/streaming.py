"""Logique temps réel (Level 3) — fonctions pures, testables sans Kafka.

Job 1 (raw -> silver) :
  dedup_stream()      déduplication sur la clé métier dans une fenêtre de 10 min
  parse_raw()         JSON NiFi -> lignes typées au contrat Bronze + rejets (DLQ)
  to_kafka_json()     sérialisation JSON des lignes Silver (topics silver-*)
Job 2 (silver -> gold) :
  large_txn_bursts()  fraude 1 : >= 2 transactions > 500 000 XOF d'un compte en 5 min
  unusual_country()   fraude 2 : paiement mobile émis hors du pays du client
  claims_over_premium() fraude 3 : sinistre > 3 x primes annuelles de la police
  aml_events()        virement > seuil déclaratif BCEAO (1 M XOF) / Ghana (5 000 GHS)
  liquidity_flows() + liquidity_alerts()  sorties nettes par pays vs réserve de liquidité
"""
from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (StringType, StructField, StructType)

from .fx import XOF_PER_EUR
from .schemas import CORRUPT_COL, DatasetSpec
from .validation import REASON_COL, prepare_batch

# --------------------------------------------------------------------------- topics
RAW_TOPICS = {
    "bank_transactions": "raw-bank-transactions",
    "insurance_operations": "raw-insurance-operations",
    "mobile_money_payments": "raw-mobile-money-payments",
    "loan_repayments": "raw-loan-repayments",
}
SILVER_TOPICS = {
    "bank_transactions": "silver-bank-transactions",
    "insurance_operations": "silver-insurance-operations",
    "mobile_money_payments": "silver-mobile-money",
    "loan_repayments": "silver-loan-repayments",
}
DLQ_TOPIC = "dlq-financial-events"
FRAUD_TOPIC = "gold-fraud-alerts"
AML_TOPIC = "gold-aml-events"
LIQUIDITY_TOPIC = "gold-liquidity-alerts"

# --------------------------------------------------------------------------- seuils
DEDUP_WINDOW = "10 minutes"
FRAUD_WINDOW, FRAUD_SLIDE = "5 minutes", "1 minute"
LARGE_TXN_XOF = 500_000
LARGE_TXN_EUR = round(LARGE_TXN_XOF / XOF_PER_EUR, 2)           # 762,25 EUR
AML_THRESHOLDS_LOCAL = {"XOF": 1_000_000, "GHS": 5_000}          # seuils déclaratifs
CLAIM_PREMIUM_MULTIPLE = 3
BCEAO_RESERVE_RATIO = 0.03       # réserves obligatoires : 3 % des dépôts
LIQUIDITY_ALERT_SHARE = 0.5      # alerte si les sorties nettes (5 min) > 50 % de la réserve

KEY_FIELDS = ("transaction_id", "operation_id", "payment_id", "repayment_id")
TS_OUT = "yyyy-MM-dd'T'HH:mm:ss.SSS"


# =========================================================================== Job 1
def event_key(value: Column, topic: Column, partition: Column, offset: Column) -> Column:
    """Clé métier du message ; à défaut (JSON illisible), ses coordonnées Kafka — sinon
    tous les messages malformés auraient la même clé NULL et seraient fusionnés."""
    keys = [F.get_json_object(value, f"$.{k}") for k in KEY_FIELDS]
    return F.coalesce(*keys, F.concat_ws(":", topic, partition.cast("string"),
                                         offset.cast("string")))


def dedup_stream(kafka: DataFrame) -> DataFrame:
    """Colonnes utiles du message Kafka + déduplication sur (topic, clé) sur 10 min.

    Le temps de référence est l'horodatage Kafka (≈ heure d'ingestion par NiFi),
    monotone : un rejeu de fichier dans les 10 min est absorbé, et les événements
    métier anciens (données historiques rejouées) ne sont pas écartés comme « tardifs »."""
    return (kafka
            .select(F.col("value").cast("string").alias("value"), "topic", "partition",
                    "offset", F.col("timestamp").alias("kafka_ts"))
            .withColumn("event_key", event_key(F.col("value"), F.col("topic"),
                                               F.col("partition"), F.col("offset")))
            .withWatermark("kafka_ts", DEDUP_WINDOW)
            .dropDuplicatesWithinWatermark(["topic", "event_key"]))


def _string_schema(spec: DatasetSpec) -> StructType:
    """Schéma de lecture JSON : tout en chaîne (CSV -> JSON par NiFi), + champs NiFi."""
    return StructType([StructField(f.name, StringType()) for f in spec.fields]
                      + [StructField(n, StringType()) for n in
                         ("ingestion_timestamp", "source_file", "landed_at")]
                      + [StructField(CORRUPT_COL, StringType())])


def parse_raw(messages: DataFrame, spec: DatasetSpec, batch_id) -> tuple[DataFrame, DataFrame]:
    """Messages JSON d'un topic raw-* -> (lignes valides au contrat Bronze, rejets DLQ).

    Réutilise exactement la validation du batch (Level 1) : mêmes règles, mêmes motifs.
    Une valeur non convertible dans son type (ex. montant "abc") rend la ligne malformée."""
    parsed = messages.withColumn(
        "_j", F.from_json("value", _string_schema(spec),
                          {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": CORRUPT_COL}))
    typed_cols, cast_errors = [], []
    for f in spec.fields:
        raw = F.col(f"_j.{f.name}")
        raw = F.when(F.trim(raw) == "", F.lit(None)).otherwise(raw)   # "" -> NULL
        typed = raw.cast(f.dataType) if not isinstance(f.dataType, StringType) else raw
        typed_cols.append(typed.alias(f.name))
        cast_errors.append(raw.isNotNull() & typed.isNull())
    malformed = F.col("_j").isNull() | F.col(f"_j.{CORRUPT_COL}").isNotNull()
    for err in cast_errors:
        malformed = malformed | err
    source = F.concat(F.lit("kafka://"), F.col("topic"), F.lit("/"),
                      F.col("partition").cast("string"), F.lit("/"),
                      F.col("offset").cast("string"))
    typed = parsed.select(
        *typed_cols,
        F.when(malformed, F.col("value")).alias(CORRUPT_COL),
        source.alias("_source_file"),
        F.col("_j.source_file").alias("_landing_file"),
        F.coalesce(F.to_timestamp("_j.ingestion_timestamp"), F.col("kafka_ts"))
         .alias("_ingested_at"),
        F.to_timestamp("_j.landed_at").alias("_landed_at"),
        "kafka_ts")
    batch = prepare_batch(typed, spec, f"stream-{batch_id}")
    # Un doublon exact dans le micro-lot n'est pas une erreur : il n'alimente pas la DLQ.
    dlq = batch.rejected.filter(F.col(REASON_COL) != "DUPLICATE_IN_BATCH")
    return batch.valid, dlq


def dlq_records(rejected: DataFrame, dataset: str, stage: str) -> DataFrame:
    """Message DLQ : motif + message d'origine, jamais d'abandon silencieux."""
    return rejected.select(
        F.col("record_key").alias("key"),
        F.to_json(F.struct(
            F.lit(stage).alias("stage"), F.lit(dataset).alias("dataset"),
            F.col("source_file").alias("source"), F.col("record_key"),
            F.col("reject_reason"), F.col("raw_record"),
            F.date_format(F.current_timestamp(), TS_OUT).alias("detected_at"))).alias("value"))


def quarantine_to_dlq(quarantine: DataFrame) -> DataFrame:
    """Clés orphelines détectées en Silver (compte, client, agence inconnus) -> DLQ."""
    return quarantine.select(
        F.col("record_key").alias("key"),
        F.to_json(F.struct(
            F.lit("silver").alias("stage"), "dataset", "record_key", "country_code",
            F.col("reason").alias("reject_reason"),
            F.date_format("detected_at", TS_OUT).alias("detected_at"))).alias("value"))


def to_kafka_json(df: DataFrame, key: str) -> DataFrame:
    """(key, value JSON) ; horodatages au format ISO 8601 sans fuseau (UTC), lisibles
    par le décodeur JSON du connecteur Kafka de Trino."""
    cols = []
    for f in df.schema.fields:
        if f.dataType.typeName() == "timestamp":
            cols.append(F.date_format(F.col(f.name), TS_OUT).alias(f.name))
        elif f.dataType.typeName() == "date":
            cols.append(F.date_format(F.col(f.name), "yyyy-MM-dd").alias(f.name))
        else:
            cols.append(F.col(f.name))
    return df.select(F.col(key).cast("string").alias("key"),
                     F.to_json(F.struct(*cols)).alias("value"))


# =========================================================================== Job 2
SILVER_JSON_SCHEMAS = {
    "bank_transactions": [
        ("transaction_id", "string"), ("timestamp", "string"), ("country_code", "string"),
        ("entity_type", "string"), ("transaction_type", "string"),
        ("transaction_status", "string"), ("amount", "double"), ("currency", "string"),
        ("amount_eur", "double"), ("account_key", "string"), ("customer_key", "string"),
        ("is_success", "boolean")],
    "mobile_money_payments": [
        ("payment_id", "string"), ("timestamp", "string"), ("country_code", "string"),
        ("sender_country", "string"), ("receiver_country", "string"),
        ("sender_home_country", "string"), ("payment_type", "string"), ("status", "string"),
        ("amount", "double"), ("currency", "string"), ("amount_eur", "double"),
        ("sender_key", "string"), ("is_success", "boolean")],
    "insurance_operations": [
        ("operation_id", "string"), ("timestamp", "string"), ("country_code", "string"),
        ("operation_type", "string"), ("product_line", "string"), ("amount", "double"),
        ("currency", "string"), ("amount_eur", "double"), ("account_key", "string"),
        ("customer_key", "string")],
}


def parse_silver(kafka_value: DataFrame, dataset: str) -> DataFrame:
    """Messages silver-* -> colonnes typées (timestamp en TimestampType)."""
    schema = StructType([StructField(n, _type(t)) for n, t in SILVER_JSON_SCHEMAS[dataset]])
    df = kafka_value.select(F.from_json(F.col("value").cast("string"), schema).alias("s"),
                            F.col("timestamp").alias("kafka_ts")).select("s.*", "kafka_ts")
    return df.withColumn("timestamp", F.to_timestamp("timestamp")) \
             .filter(F.col("timestamp").isNotNull())


def _type(name: str):
    from pyspark.sql.types import BooleanType, DoubleType
    return {"string": StringType(), "double": DoubleType(), "boolean": BooleanType()}[name]


def _alert(rule: str, severity: str, subject: Column, event_time: Column, amount_eur: Column,
           details: Column, *, entity: Column, country: Column,
           window_start: Column | None = None, window_end: Column | None = None,
           amount_local: Column | None = None, currency: Column | None = None,
           threshold: Column | None = None, id_parts: list[Column] | None = None) -> list[Column]:
    parts = id_parts or [subject, event_time.cast("string")]
    return [
        F.sha2(F.concat_ws("|", F.lit(rule), *[p.cast("string") for p in parts]), 256)
         .alias("alert_id"),
        F.lit(rule).alias("rule"), F.lit(severity).alias("severity"),
        country.alias("country_code"), entity.alias("entity_type"),
        subject.alias("subject_key"),
        (window_start if window_start is not None else F.lit(None).cast("timestamp"))
         .alias("window_start"),
        (window_end if window_end is not None else F.lit(None).cast("timestamp"))
         .alias("window_end"),
        event_time.alias("event_time"),
        F.round(amount_eur, 2).alias("amount_eur"),
        (amount_local if amount_local is not None else F.lit(None).cast("double"))
         .alias("amount_local"),
        (currency if currency is not None else F.lit(None).cast("string")).alias("currency"),
        (threshold if threshold is not None else F.lit(None).cast("double")).alias("threshold"),
        F.to_json(details).alias("details"),
        F.current_timestamp().alias("detected_at"),
    ]


def large_txn_bursts(bank: DataFrame, min_count: int = 2) -> DataFrame:
    """Fraude 1 : >= 2 transactions > 500 000 XOF (équivalent EUR) depuis un même compte
    dans une fenêtre glissante de 5 min (pas de 1 min). Agrégat en temps événement."""
    agg = (bank.filter(F.col("amount_eur") > LARGE_TXN_EUR)
           .groupBy(F.window("timestamp", FRAUD_WINDOW, FRAUD_SLIDE).alias("w"),
                    "account_key", "country_code")
           .agg(F.count("*").alias("nb"), F.sum("amount_eur").alias("total_eur"),
                F.min("timestamp").alias("first_ts"), F.max("timestamp").alias("last_ts"),
                F.collect_set("transaction_id").alias("txn_ids"))
           .filter(F.col("nb") >= min_count))
    return agg.select(*_alert(
        "MULTIPLE_LARGE_TXN", "HIGH", F.col("account_key"), F.col("last_ts"),
        F.col("total_eur"),
        F.struct(F.col("nb").alias("nb_transactions"), F.col("txn_ids").alias("transaction_ids"),
                 F.lit(LARGE_TXN_XOF).alias("threshold_xof_per_txn")),
        entity=F.lit("BANK"), country=F.col("country_code"),
        window_start=F.col("w.start"), window_end=F.col("w.end"),
        threshold=F.lit(LARGE_TXN_EUR),
        # même rafale vue dans plusieurs fenêtres glissantes -> même alerte
        id_parts=[F.col("account_key"), F.col("first_ts").cast("string")]))


def unusual_country(mm: DataFrame) -> DataFrame:
    """Fraude 2 : paiement mobile émis depuis un pays différent du pays du client."""
    sus = mm.filter(F.col("sender_home_country").isNotNull()
                    & (F.col("sender_country") != F.col("sender_home_country")))
    return sus.select(*_alert(
        "UNUSUAL_COUNTRY", "MEDIUM", F.col("sender_key"), F.col("timestamp"),
        F.col("amount_eur"),
        F.struct("payment_id", "sender_country", "sender_home_country", "payment_type"),
        entity=F.lit("MOBILE_MONEY"), country=F.col("sender_home_country"),
        amount_local=F.col("amount"), currency=F.col("currency"),
        id_parts=[F.col("payment_id")]))


def premiums_by_account(history: DataFrame, as_of: Column | None = None) -> DataFrame:
    """Primes (émission + renouvellement) des 365 derniers jours, par police."""
    h = history.filter(F.col("operation_type").isin("PREMIUM_PAYMENT", "POLICY_RENEWAL"))
    ref = as_of if as_of is not None else F.current_timestamp()
    h = h.filter(F.col("timestamp") > ref - F.expr("INTERVAL 365 DAYS"))
    return (h.dropDuplicates(["operation_id"])
            .groupBy("account_key").agg(F.sum("amount_eur").alias("annual_premium_eur")))


def claims_over_premium(claims_batch: DataFrame, premiums: DataFrame) -> DataFrame:
    """Fraude 3 : montant d'un sinistre > 3 x la prime annuelle versée sur la police."""
    claims = claims_batch.filter(F.col("operation_type").isin("CLAIM_SUBMISSION",
                                                               "CLAIM_PAYMENT"))
    j = claims.join(premiums, "account_key", "inner").filter(
        (F.col("annual_premium_eur") > 0)
        & (F.col("amount_eur") > CLAIM_PREMIUM_MULTIPLE * F.col("annual_premium_eur")))
    return j.select(*_alert(
        "CLAIM_EXCEEDS_PREMIUM", "HIGH", F.col("account_key"), F.col("timestamp"),
        F.col("amount_eur"),
        F.struct("operation_id", "operation_type", "product_line",
                 F.round("annual_premium_eur", 2).alias("annual_premium_eur"),
                 F.round(F.col("amount_eur") / F.col("annual_premium_eur"), 2)
                  .alias("claim_to_premium_ratio")),
        entity=F.lit("INSURANCE"), country=F.col("country_code"),
        amount_local=F.col("amount"), currency=F.col("currency"),
        threshold=F.col("annual_premium_eur") * CLAIM_PREMIUM_MULTIPLE,
        id_parts=[F.col("operation_id")]))


def _aml_threshold(currency: Column) -> Column:
    expr = F.lit(None).cast("double")
    for cur, amount in AML_THRESHOLDS_LOCAL.items():
        expr = F.when(currency == cur, F.lit(float(amount))).otherwise(expr)
    return expr


def aml_events(bank: DataFrame, mm: DataFrame) -> DataFrame:
    """Virements (bancaires ou mobile money) au-delà du seuil déclaratif, en devise locale."""
    b = bank.filter(F.col("transaction_type").isin("TRANSFER", "INTERNATIONAL_WIRE")).select(
        F.col("transaction_id").alias("event_id"), "timestamp", "country_code",
        F.lit("BANK").alias("entity_type"), F.col("transaction_type").alias("operation"),
        "amount", "currency", "amount_eur", F.col("account_key").alias("subject_key"))
    m = mm.filter(F.col("payment_type").isin("P2P", "CROSS_BORDER_TRANSFER")).select(
        F.col("payment_id").alias("event_id"), "timestamp", "country_code",
        F.lit("MOBILE_MONEY").alias("entity_type"), F.col("payment_type").alias("operation"),
        "amount", "currency", "amount_eur", F.col("sender_key").alias("subject_key"))
    ev = b.unionByName(m).withColumn("_thr", _aml_threshold(F.col("currency")))
    ev = ev.filter(F.col("_thr").isNotNull() & (F.col("amount") >= F.col("_thr")))
    return ev.select(*_alert(
        "AML_THRESHOLD_EXCEEDED", "HIGH", F.col("subject_key"), F.col("timestamp"),
        F.col("amount_eur"), F.struct("event_id", "operation"),
        entity=F.col("entity_type"), country=F.col("country_code"),
        amount_local=F.col("amount"), currency=F.col("currency"), threshold=F.col("_thr"),
        id_parts=[F.col("event_id")]))


OUTFLOW_TYPES = ("WITHDRAWAL", "TRANSFER", "INTERNATIONAL_WIRE")


def liquidity_flows(bank: DataFrame) -> DataFrame:
    """Sorties / entrées de fonds réussies par pays, fenêtre glissante 5 min (pas 1 min)."""
    ok = bank.filter(F.col("is_success"))
    out = F.when(F.col("transaction_type").isin(*OUTFLOW_TYPES), F.col("amount_eur"))
    inn = F.when(F.col("transaction_type") == "DEPOSIT", F.col("amount_eur"))
    return (ok.groupBy(F.window("timestamp", FRAUD_WINDOW, FRAUD_SLIDE).alias("w"),
                       "country_code")
            .agg(F.coalesce(F.sum(out), F.lit(0.0)).alias("outflows_eur"),
                 F.coalesce(F.sum(inn), F.lit(0.0)).alias("inflows_eur"),
                 F.count("*").alias("nb_transactions"))
            .withColumn("net_outflow_eur", F.col("outflows_eur") - F.col("inflows_eur")))


def liquidity_reserves(accounts: DataFrame) -> DataFrame:
    """Réserve de liquidité de référence par pays : 3 % des dépôts bancaires (Silver)."""
    dep = accounts.filter((F.col("status") == "ACTIVE") & (F.col("entity_type") == "BANK")
                          & F.col("account_type").isin("CURRENT", "SAVINGS"))
    return (dep.groupBy("country_code")
            .agg(F.sum(F.greatest(F.col("balance_eur").cast("double"), F.lit(0.0)))
                 .alias("deposits_eur"))
            .withColumn("reserve_eur", F.col("deposits_eur") * BCEAO_RESERVE_RATIO))


def liquidity_alerts(flows: DataFrame, reserves: DataFrame) -> DataFrame:
    """Alerte si les sorties nettes de la fenêtre dépassent 50 % de la réserve du pays."""
    j = flows.join(reserves, "country_code", "inner").withColumn(
        "_thr", F.col("reserve_eur") * LIQUIDITY_ALERT_SHARE)
    j = j.filter(F.col("net_outflow_eur") > F.col("_thr"))
    return j.select(*_alert(
        "LIQUIDITY_COVERAGE_BREACH", "CRITICAL", F.col("country_code"), F.col("w.end"),
        F.col("net_outflow_eur"),
        F.struct(F.round("outflows_eur", 2).alias("outflows_eur"),
                 F.round("inflows_eur", 2).alias("inflows_eur"),
                 F.round("reserve_eur", 2).alias("reserve_eur"),
                 F.round(F.col("net_outflow_eur") / F.col("reserve_eur"), 4)
                  .alias("share_of_reserve")),
        entity=F.lit("BANK"), country=F.col("country_code"),
        window_start=F.col("w.start"), window_end=F.col("w.end"), threshold=F.col("_thr"),
        id_parts=[F.col("country_code"), F.col("w.start").cast("string")]))


def suppress_repeats(alerts: DataFrame, recent: DataFrame,
                     minutes: int = 5) -> DataFrame:
    """Anti-rafale d'alertes : une même règle sur le même sujet n'est ré-émise qu'après
    `minutes` (les fenêtres glissantes voient le même incident plusieurs fois)."""
    r = recent.select(F.col("rule").alias("_r"), F.col("subject_key").alias("_s"),
                      F.col("event_time").alias("_t"), F.col("alert_id").alias("_id"))
    cond = ((F.col("rule") == F.col("_r")) & (F.col("subject_key") == F.col("_s"))
            & (F.col("alert_id") != F.col("_id"))
            & (F.abs(F.col("event_time").cast("long") - F.col("_t").cast("long"))
               < minutes * 60))
    return alerts.join(r, cond, "left_anti")
