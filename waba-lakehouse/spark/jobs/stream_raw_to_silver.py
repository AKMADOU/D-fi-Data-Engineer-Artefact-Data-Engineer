"""Level 3 — Job Structured Streaming n°1 : raw-* (Kafka) -> Silver temps réel.

    spark-submit jobs/stream_raw_to_silver.py [--trigger 5] [--max-offsets 20000]

* lit les 4 topics raw-* alimentés par NiFi (JSON) ;
* déduplique sur la clé métier (transaction_id, operation_id, ...) sur 10 min ;
* applique les règles de qualité du Level 1 (mêmes motifs de rejet) ;
* enrichit / convertit en EUR / pseudonymise avec les transformations du Level 2 ;
* écrit la vue Silver temps réel dans Iceberg (silver.rt_<dataset>, MERGE idempotent)
  et dans les topics silver-* ; les rejets et orphelins partent dans dlq-financial-events.

Tolérance aux pannes : offsets et état de déduplication sont dans le checkpoint
(STREAM_CHECKPOINT_DIR) ; un micro-lot rejoué réécrit les mêmes clés (MERGE).
"""
from __future__ import annotations

import argparse
import os
import sys

from common import streaming as S
from common.jobutils import j, setup_logging
from common.lakehouse import ensure_audit_tables
from common.metrics import StreamMetrics
from common.session import build_spark, env
from common.stream_pipeline import LakehouseSinks, LookupCache, bronze_lookups, process_raw_batch

log = setup_logging("stream_raw_to_silver")


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--trigger", type=int, default=int(os.environ.get("STREAM_TRIGGER_S", "5")),
                   help="Intervalle de micro-lot (s)")
    p.add_argument("--max-offsets", type=int, default=20000,
                   help="Messages max par micro-lot (back-pressure)")
    p.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    args = p.parse_args(argv)

    secret = env("PII_HASH_SECRET")
    bootstrap = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    checkpoint = env("STREAM_CHECKPOINT_DIR", "/checkpoints") + "/raw_to_silver"

    spark = build_spark("waba-stream-raw-to-silver")
    ensure_audit_tables(spark)
    if not spark.catalog.tableExists("silver.fx_rates"):
        raise SystemExit("silver.fx_rates absente : lancer une fois le pipeline batch "
                         "(make medallion ou dag_bronze_to_silver) avant le streaming")
    lookups = LookupCache(lambda: bronze_lookups(spark),
                          ttl_s=int(os.environ.get("STREAM_LOOKUP_TTL_S", "300")))
    sinks = LakehouseSinks(spark, bootstrap)
    # Level 4 : /metrics, /healthz, /ready pour Prometheus et les sondes Kubernetes
    metrics = StreamMetrics("raw_to_silver")
    spark.streams.addListener(metrics.listener())
    if os.environ.get("METRICS_PORT"):
        metrics.serve(int(os.environ["METRICS_PORT"]))

    kafka = (spark.readStream.format("kafka")
             .option("kafka.bootstrap.servers", bootstrap)
             .option("subscribe", ",".join(S.RAW_TOPICS.values()))
             .option("startingOffsets", args.starting_offsets)
             .option("maxOffsetsPerTrigger", args.max_offsets)
             .option("failOnDataLoss", "false")
             .load())

    def on_batch(df, batch_id):
        rep = process_raw_batch(df, batch_id, lookups, sinks, secret)
        metrics.country_lag(rep.lag_by_country)
        for dataset, n in rep.dlq_by_dataset.items():
            metrics.dlq(dataset, n)
        if rep.rows_in:
            log.info(j(job="raw_to_silver", batch_id=batch_id, rows_in=rep.rows_in,
                       silver_rows=rep.valid, dlq=rep.dlq, quarantined=rep.quarantined,
                       max_lag_s=rep.max_lag_s, lag_by_country=rep.lag_by_country,
                       lag_ok=rep.max_lag_s is not None and rep.max_lag_s < 30))

    query = (S.dedup_stream(kafka).writeStream
             .queryName("raw_to_silver")
             .foreachBatch(on_batch)
             .option("checkpointLocation", checkpoint)
             .trigger(processingTime=f"{args.trigger} seconds")
             .start())
    log.info(j(status="STARTED", job="raw_to_silver", topics=list(S.RAW_TOPICS.values()),
               checkpoint=checkpoint, trigger_s=args.trigger))
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
