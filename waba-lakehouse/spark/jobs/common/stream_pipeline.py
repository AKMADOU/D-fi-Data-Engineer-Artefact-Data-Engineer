"""Orchestration des micro-lots des deux jobs Structured Streaming (Level 3).

Les écritures passent par un objet `Sinks` : en production Kafka + Iceberg,
en test un collecteur en mémoire. La logique métier reste dans `streaming.py`.
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from . import streaming as S
from .schemas import DATASETS
from .silver_transforms import (latest, silver_bank_transactions, silver_insurance_operations,
                                silver_loan_repayments, silver_mobile_money)

SILVER_KEYS = {"bank_transactions": "transaction_id", "insurance_operations": "operation_id",
               "mobile_money_payments": "payment_id", "loan_repayments": "repayment_id"}
RT_TABLE = "silver.rt_{}"
ALERT_TABLES = {"fraud": "gold.fraud_alerts", "aml": "gold.aml_events",
                "liquidity": "gold.liquidity_alerts"}
ALERT_TOPICS = {"fraud": S.FRAUD_TOPIC, "aml": S.AML_TOPIC, "liquidity": S.LIQUIDITY_TOPIC}


# --------------------------------------------------------------------------- sinks
class Sinks:
    """Interface des sorties d'un micro-lot."""

    def kafka(self, df: DataFrame, topic: str) -> None:            # (key, value)
        raise NotImplementedError

    def upsert(self, df: DataFrame, table: str, key: str,
               partitioning: tuple[str, ...]) -> None:
        raise NotImplementedError

    def append(self, df: DataFrame, table: str) -> None:
        raise NotImplementedError

    def recent_alerts(self, table: str, minutes: int) -> DataFrame | None:
        return None


class LakehouseSinks(Sinks):
    """Kafka (batch write) + Iceberg (MERGE idempotent : un micro-lot rejoué après
    une panne réécrit les mêmes clés au lieu de dupliquer)."""

    def __init__(self, spark: SparkSession, bootstrap: str):
        self.spark, self.bootstrap = spark, bootstrap
        self._created: set[str] = set()
        # Les requêtes d'un même job (ex. rafales + règles sans état) écrivent dans la même
        # table Gold depuis des threads différents : on sérialise les commits Iceberg pour
        # éviter les conflits d'écriture concurrente (MERGE copy-on-write).
        self._lock = threading.Lock()

    def kafka(self, df, topic):
        (df.select(F.col("key").cast("string"), F.col("value").cast("string"))
         .write.format("kafka").option("kafka.bootstrap.servers", self.bootstrap)
         .option("topic", topic).save())

    def upsert(self, df, table, key, partitioning):
        with self._lock:
            self._upsert(df, table, key, partitioning)

    def _upsert(self, df, table, key, partitioning):
        from .lakehouse import create_table_like
        if table not in self._created:
            create_table_like(self.spark, df, table, partitioning,
                              "Level 3 - alimentée en temps réel par Structured Streaming")
            self._created.add(table)
        view = f"_upsert_{table.replace('.', '_')}_{threading.get_ident()}"
        df.createOrReplaceTempView(view)
        # foreachBatch : le DataFrame appartient à une session clonée -> même session ici
        df.sparkSession.sql(f"MERGE INTO {table} t USING {view} s ON t.`{key}` = s.`{key}` "
                       "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *")

    def append(self, df, table):
        df.writeTo(table).append()

    def recent_alerts(self, table, minutes):
        if not self.spark.catalog.tableExists(table):
            return None
        return self.spark.table(table).filter(
            F.col("detected_at") > F.current_timestamp() - F.expr(f"INTERVAL {minutes} MINUTES"))


@dataclass
class MemorySinks(Sinks):
    """Collecteur pour les tests."""
    topics: dict[str, list] = field(default_factory=dict)
    tables: dict[str, dict] = field(default_factory=dict)
    appended: dict[str, list] = field(default_factory=dict)

    def kafka(self, df, topic):
        self.topics.setdefault(topic, []).extend(r.asDict() for r in df.collect())

    def upsert(self, df, table, key, partitioning):
        t = self.tables.setdefault(table, {})
        for r in df.collect():
            t[r[key]] = r.asDict()

    def append(self, df, table):
        self.appended.setdefault(table, []).extend(r.asDict() for r in df.collect())


# --------------------------------------------------------------------------- lookups
class LookupCache:
    """Référentiels (Bronze) et taux de change, mis en cache et rafraîchis toutes les
    `ttl_s` secondes : un compte créé par le batch est visible sans redémarrer le job."""

    def __init__(self, loader: Callable[[], dict[str, DataFrame]], ttl_s: int = 300):
        self.loader, self.ttl_s = loader, ttl_s
        self._frames: dict[str, DataFrame] = {}
        self._loaded_at = 0.0

    def get(self) -> dict[str, DataFrame]:
        if not self._frames or time.time() - self._loaded_at > self.ttl_s:
            for df in self._frames.values():
                df.unpersist()
            self._frames = {k: v.cache() for k, v in self.loader().items()}
            self._loaded_at = time.time()
        return self._frames


def bronze_lookups(spark: SparkSession) -> dict[str, DataFrame]:
    out = {n: latest(spark.table(f"bronze.{n}"), DATASETS[n].key)
           for n in ("customers", "accounts", "branches", "products")}
    out["fx"] = spark.table("silver.fx_rates")
    return out


# =========================================================================== Job 1
@dataclass
class BatchReport:
    rows_in: int = 0
    valid: dict[str, int] = field(default_factory=dict)
    dlq: int = 0
    quarantined: int = 0
    max_lag_s: float | None = None
    # Latence ingestion Kafka -> Silver écrit, par pays (secondes, max du micro-lot)
    lag_by_country: dict[str, float] = field(default_factory=dict)
    dlq_by_dataset: dict[str, int] = field(default_factory=dict)


def build_silver_rt(dataset: str, valid: DataFrame, lk: dict[str, DataFrame],
                    secret: str) -> tuple[DataFrame, DataFrame]:
    c, a, b, p, fx = lk["customers"], lk["accounts"], lk["branches"], lk["products"], lk["fx"]
    if dataset == "bank_transactions":
        return silver_bank_transactions(valid, a, c, b, fx, secret)
    if dataset == "insurance_operations":
        return silver_insurance_operations(valid, a, c, fx, secret)
    if dataset == "mobile_money_payments":
        return silver_mobile_money(valid, c, fx, secret)
    return silver_loan_repayments(valid, a, c, p, fx, secret)


def process_raw_batch(batch: DataFrame, batch_id: int, lookups: LookupCache, sinks: Sinks,
                      secret: str, audit_table: str | None = "audit.rejected_records"
                      ) -> BatchReport:
    """Micro-lot du Job 1 : validation -> Silver temps réel (Iceberg + Kafka) ; rejets -> DLQ.

    Ordre : Iceberg d'abord, Kafka ensuite. Le Job 2 lit Kafka ; quand il reçoit un
    événement, l'historique Iceberg (primes d'assurance) le contient déjà."""
    rep = BatchReport()
    batch = batch.persist()
    rep.rows_in = batch.count()
    if not rep.rows_in:
        batch.unpersist()
        return rep
    lag = batch.agg(F.max(F.unix_timestamp(F.current_timestamp())
                          - F.unix_timestamp("kafka_ts"))).first()[0]
    rep.max_lag_s = float(lag) if lag is not None else None
    lk = lookups.get()
    topics = {r["topic"] for r in batch.select("topic").distinct().collect()}
    dlq_parts: list[DataFrame] = []
    for dataset, topic in S.RAW_TOPICS.items():
        if topic not in topics:
            continue
        valid, rejected = S.parse_raw(batch.filter(F.col("topic") == topic),
                                      DATASETS[dataset], batch_id)
        valid = valid.persist()
        rejected = rejected.persist()
        silver, quarantine = build_silver_rt(dataset, valid, lk, secret)
        silver = silver.persist()
        n = silver.count()
        rep.valid[dataset] = n
        if n:
            sinks.upsert(silver, RT_TABLE.format(dataset), SILVER_KEYS[dataset],
                         ("country_code", "days(timestamp)"))
            for r in (valid.groupBy("country_code")
                      .agg(F.max(F.unix_timestamp(F.current_timestamp())
                                 - F.unix_timestamp("kafka_ts")).alias("lag"))
                      .collect()):
                if r["country_code"] and r["lag"] is not None:
                    rep.lag_by_country[r["country_code"]] = max(
                        float(r["lag"]), rep.lag_by_country.get(r["country_code"], 0.0))
            sinks.kafka(S.to_kafka_json(silver, SILVER_KEYS[dataset]), S.SILVER_TOPICS[dataset])
        n_rej = rejected.count()
        if n_rej:
            dlq_parts.append(S.dlq_records(rejected, dataset, "raw"))
            if audit_table:
                sinks.append(rejected, audit_table)
        q = quarantine.persist()
        n_q = q.count()
        if n_q:
            dlq_parts.append(S.quarantine_to_dlq(q))
        rep.dlq += n_rej + n_q
        rep.dlq_by_dataset[dataset] = n_rej + n_q
        rep.quarantined += n_q
        for df in (valid, rejected, silver, q):
            df.unpersist()
    if dlq_parts:
        dlq = dlq_parts[0]
        for part in dlq_parts[1:]:
            dlq = dlq.unionByName(part)
        sinks.kafka(dlq, S.DLQ_TOPIC)
    batch.unpersist()
    return rep


# =========================================================================== Job 2
def dedupe_alerts(alerts: DataFrame, recent: DataFrame | None) -> DataFrame:
    """Une alerte par (règle, sujet) dans le micro-lot, puis anti-rafale vs l'historique."""
    w = Window.partitionBy("rule", "subject_key").orderBy(F.col("amount_eur").desc(),
                                                          F.col("alert_id"))
    one = alerts.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
    if recent is not None:
        one = S.suppress_repeats(one, recent)
    return one


def emit_alerts(alerts: DataFrame, kind: str, sinks: Sinks) -> int:
    table = ALERT_TABLES[kind]
    alerts = dedupe_alerts(alerts, sinks.recent_alerts(table, 60)).persist()
    n = alerts.count()
    if n:
        sinks.upsert(alerts, table, "alert_id", ("country_code", "days(event_time)"))
        sinks.kafka(S.to_kafka_json(alerts, "alert_id"), ALERT_TOPICS[kind])
    alerts.unpersist()
    return n


def process_event_batch(bank: DataFrame, mm: DataFrame, ins: DataFrame,
                        premium_history: DataFrame, sinks: Sinks) -> dict[str, int]:
    """Règles sans état inter-lots : pays inhabituel, sinistre vs primes, seuil AML."""
    ins = ins.persist()
    premiums = S.premiums_by_account(
        premium_history.select("operation_id", "operation_type", "timestamp", "amount_eur",
                               "account_key")
        .unionByName(ins.select("operation_id", "operation_type", "timestamp", "amount_eur",
                                "account_key")))
    fraud = S.unusual_country(mm).unionByName(S.claims_over_premium(ins, premiums))
    out = {"fraud": emit_alerts(fraud, "fraud", sinks),
           "aml": emit_alerts(S.aml_events(bank, mm), "aml", sinks)}
    ins.unpersist()
    return out
