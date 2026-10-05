"""Level 3 — Job Structured Streaming n°2 : silver-* (Kafka) -> alertes Gold.

    spark-submit jobs/stream_silver_to_gold.py [--trigger 5]

Trois requêtes streaming (checkpoints séparés) :
  * bursts     : >= 2 transactions > 500 000 XOF d'un même compte en 5 min
                 (fenêtre glissante 5 min / pas 1 min, watermark 10 min)      -> fraude
  * liquidity  : sorties nettes par pays sur 5 min > 50 % de la réserve
                 (3 % des dépôts, silver.accounts)                            -> liquidité
  * events     : sans état — pays inhabituel (mobile money), sinistre > 3 x
                 primes annuelles, virements au-delà du seuil déclaratif     -> fraude / AML

Sorties : topics gold-fraud-alerts, gold-aml-events, gold-liquidity-alerts
et tables Iceberg gold.fraud_alerts, gold.aml_events, gold.liquidity_alerts (MERGE sur alert_id).
"""
from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import functions as F

from common import streaming as S
from common.jobutils import j, setup_logging
from common.metrics import StreamMetrics
from common.session import build_spark, env
from common.stream_pipeline import (LakehouseSinks, LookupCache, emit_alerts,
                                    process_event_batch)

log = setup_logging("stream_silver_to_gold")
WATERMARK = "10 minutes"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--trigger", type=int, default=int(os.environ.get("STREAM_TRIGGER_S", "5")))
    p.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    args = p.parse_args(argv)

    bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    base = env("STREAM_CHECKPOINT_DIR", "/checkpoints") + "/silver_to_gold"
    spark = build_spark("waba-stream-silver-to-gold")
    sinks = LakehouseSinks(spark, bootstrap)
    trigger = f"{args.trigger} seconds"
    # Level 4 : /metrics (dont le lag Kafka par requête), /healthz, /ready
    metrics = StreamMetrics("silver_to_gold")
    spark.streams.addListener(metrics.listener())
    if os.environ.get("METRICS_PORT"):
        metrics.serve(int(os.environ["METRICS_PORT"]))
    # Back-pressure de la requête des règles sans état (fraude 2-3, AML)
    events_max_offsets = int(os.environ.get("STREAM_EVENTS_MAX_OFFSETS", "20000"))

    def source(*topics, max_offsets: int | None = None):
        reader = (spark.readStream.format("kafka")
                  .option("kafka.bootstrap.servers", bootstrap)
                  .option("subscribe", ",".join(topics))
                  .option("startingOffsets", args.starting_offsets)
                  .option("failOnDataLoss", "false"))
        if max_offsets:
            reader = reader.option("maxOffsetsPerTrigger", max_offsets)
        return reader.load()

    # Réserves de liquidité (Silver batch) : rafraîchies toutes les 15 min.
    def load_reserves():
        # Tant que le batch n'a pas produit silver.accounts : pas de réserve connue, donc
        # pas d'alerte de liquidité, mais les règles de fraude et AML continuent de tourner.
        if not spark.catalog.tableExists("silver.accounts"):
            log.warning(j(job="silver_to_gold", warning="silver.accounts absente : alertes "
                          "de liquidité suspendues jusqu'au premier pipeline batch"))
            return {"reserves": spark.createDataFrame(
                [], "country_code string, deposits_eur double, reserve_eur double")}
        return {"reserves": S.liquidity_reserves(spark.table("silver.accounts"))}
    reserves = LookupCache(load_reserves, ttl_s=900)

    # Historique des primes : Silver batch + Silver temps réel (rafraîchi chaque minute).
    def load_premiums():
        cols = ["operation_id", "operation_type", "timestamp", "amount_eur", "account_key"]
        hist = None
        for t in ("silver.insurance_operations", "silver.rt_insurance_operations"):
            if spark.catalog.tableExists(t):
                df = (spark.table(t).filter(F.col("operation_type").isin(
                    "PREMIUM_PAYMENT", "POLICY_RENEWAL"))
                    .filter(F.col("timestamp") > F.current_timestamp() - F.expr("INTERVAL 366 DAYS"))
                    .select(*cols))
                hist = df if hist is None else hist.unionByName(df)
        if hist is None:
            hist = spark.createDataFrame([], "operation_id string, operation_type string, "
                                             "timestamp timestamp, amount_eur decimal(20,2), "
                                             "account_key string")
        return {"premiums": hist}
    premiums = LookupCache(load_premiums, ttl_s=60)

    # 1) Rafales de grosses transactions (agrégat fenêtré, état dans le checkpoint)
    bank_stream = S.parse_silver(source(S.SILVER_TOPICS["bank_transactions"]),
                                 "bank_transactions").withWatermark("timestamp", WATERMARK)

    def on_bursts(df, batch_id):
        n = emit_alerts(df, "fraud", sinks)
        metrics.alerts("fraud", n)
        if n:
            log.info(j(job="silver_to_gold", query="bursts", batch_id=batch_id, alerts=n))

    q1 = (S.large_txn_bursts(bank_stream).writeStream.queryName("fraud_bursts")
          .outputMode("update").foreachBatch(on_bursts)
          .option("checkpointLocation", f"{base}/bursts").trigger(processingTime=trigger).start())

    # 2) Liquidité : flux nets par pays et fenêtre, comparés à la réserve
    flows = S.liquidity_flows(S.parse_silver(source(S.SILVER_TOPICS["bank_transactions"]),
                                             "bank_transactions")
                              .withWatermark("timestamp", WATERMARK))

    def on_flows(df, batch_id):
        alerts = S.liquidity_alerts(df, reserves.get()["reserves"])
        n = emit_alerts(alerts, "liquidity", sinks)
        metrics.alerts("liquidity", n)
        if n:
            log.info(j(job="silver_to_gold", query="liquidity", batch_id=batch_id, alerts=n))

    q2 = (flows.writeStream.queryName("liquidity").outputMode("update").foreachBatch(on_flows)
          .option("checkpointLocation", f"{base}/liquidity").trigger(processingTime=trigger)
          .start())

    # 3) Règles sans état (pays inhabituel, sinistres, AML)
    topics = {S.SILVER_TOPICS[d]: d for d in
              ("bank_transactions", "mobile_money_payments", "insurance_operations")}
    events = source(*topics, max_offsets=events_max_offsets)

    def on_events(df, batch_id):
        df = df.persist()
        parts = {d: S.parse_silver(df.filter(F.col("topic") == t), d) for t, d in topics.items()}
        lag = df.agg(F.max(F.unix_timestamp(F.current_timestamp())
                           - F.unix_timestamp("timestamp"))).first()[0]
        out = process_event_batch(parts["bank_transactions"], parts["mobile_money_payments"],
                                  parts["insurance_operations"],
                                  premiums.get()["premiums"], sinks)
        rows = df.count()
        df.unpersist()
        for kind, n in out.items():
            metrics.alerts(kind, n)
        if rows:
            log.info(j(job="silver_to_gold", query="events", batch_id=batch_id, rows_in=rows,
                       alerts=out, max_lag_s=lag))

    q3 = (events.writeStream.queryName("event_rules").foreachBatch(on_events)
          .option("checkpointLocation", f"{base}/events").trigger(processingTime=trigger).start())

    log.info(j(status="STARTED", job="silver_to_gold",
               queries=[q.name for q in (q1, q2, q3)], checkpoint=base))
    spark.streams.awaitAnyTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
