"""Job d'ingestion raw-landing (CSV) -> tables Iceberg bronze.* (L2) ou raw.* (L1).

Étapes, pour chaque dataset :
  1. inventaire des fichiers de raw-landing (filtrables par pays) ;
  2. saut des fichiers déjà ingérés avec succès (journal audit.ingestion_log) ;
  3. contrôle de l'en-tête (contrat de colonnes) ; fichiers non conformes mis en quarantaine ;
  4. lecture avec schéma explicite + validation ligne à ligne (devise, énumérations...) ;
  5. MERGE idempotent sur la clé métier dans raw.<dataset> ;
  6. écriture des rejets (audit.rejected_records) et du journal (audit.ingestion_log) ;
  7. archivage des fichiers traités (raw-landing -> archive), après le commit.

Idempotence à deux niveaux : le journal évite de relire un fichier connu, et le
MERGE garantit qu'une ligne déjà présente n'est jamais insérée deux fois, même
avec --force ou en rejouant les fichiers archivés (--source archive).

Usage :
    spark-submit jobs/ingest.py --dataset all                      # -> bronze.*
    spark-submit jobs/ingest.py --dataset all --namespace raw      # -> raw.* (Level 1)
    spark-submit jobs/ingest.py --dataset bank_transactions --countries CI SN
    spark-submit jobs/ingest.py --dataset bank_transactions --source archive --force
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
import uuid

from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from common.lakehouse import (INGESTION_LOG_TABLE, REJECTS_TABLE, ensure_tables,
                              merge_batch)
from common.landing import Landing, LandingFile
from common.schemas import COUNTRIES, DATASETS, INGEST_NAMESPACES, DatasetSpec
from common.session import build_spark
from common.validation import file_stats, prepare_batch, read_landing_csv

logging.basicConfig(
    level=logging.INFO, stream=sys.stdout,
    format='{"ts":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":%(message)s}')
log = logging.getLogger("ingest")


def _j(**kw) -> str:
    return json.dumps(kw, default=str)


def already_ingested(spark: SparkSession, dataset: str, target: str) -> set[tuple[str, str]]:
    # Journal propre à chaque table cible : un fichier ingéré en raw (L1) peut être
    # rejoué en bronze (L2). target_table NULL = lignes historiques du Level 1.
    target_filter = F.coalesce(F.col("target_table"), F.lit(f"raw.{dataset}")) == target
    rows = (spark.table(INGESTION_LOG_TABLE)
            .filter((F.col("dataset") == dataset) & (F.col("status") == "SUCCESS")
                    & target_filter)
            .select("source_file", "file_etag").distinct().collect())
    # Même fichier = même chemin relatif ET même contenu (ETag), quel que soit le bucket.
    return {(r.source_file.split("/", 3)[-1], r.file_etag) for r in rows}


def log_files(spark: SparkSession, batch_id: str, dataset: str, target: str,
              files: list[LandingFile], status: str, message: str, stats=None) -> None:
    by_uri = {r["_source_file"]: r for r in (stats or [])}
    rows = []
    for f in files:
        s = by_uri.get(f.uri)
        rows.append((batch_id, dataset, f.uri, f.etag,
                     s["rows_read"] if s else 0, s["rows_valid"] if s else 0,
                     s["rows_rejected"] if s else 0, status, message))
    if rows:
        (spark.createDataFrame(rows, "batch_id string, dataset string, source_file string, "
                               "file_etag string, rows_read bigint, rows_valid bigint, "
                               "rows_rejected bigint, status string, message string")
         .withColumn("processed_at", F.current_timestamp())
         .withColumn("target_table", F.lit(target))
         .writeTo(INGESTION_LOG_TABLE).append())


def ingest_dataset(spark: SparkSession, landing: Landing, spec: DatasetSpec,
                   countries: tuple[str, ...], source: str, force: bool,
                   archive: bool, namespace: str = "bronze") -> dict:
    t0 = time.time()
    batch_id = str(uuid.uuid4())
    target = spec.table_in(namespace)
    bucket = landing.archive_bucket if source == "archive" else landing.landing_bucket
    files = landing.list_files(spec, countries, bucket)
    summary = {"dataset": spec.name, "target": target, "batch_id": batch_id,
               "files_found": len(files)}

    if not force and files:
        done = already_ingested(spark, spec.name, target)
        skipped = [f for f in files if (f.key, f.etag) in done]
        files = [f for f in files if (f.key, f.etag) not in done]
        summary["files_skipped_already_ingested"] = len(skipped)
        if archive and source == "landing":
            for f in skipped:  # déjà en base : un archivage précédent avait échoué
                landing.archive(f)

    bad = [f for f in files if not landing.header_matches(f, spec)]
    if bad:
        log_files(spark, batch_id, spec.name, target, bad, "REJECTED_FILE", "header mismatch")
        if source == "landing":
            for f in bad:
                landing.archive(f, sub_prefix="_rejected_files/")
        files = [f for f in files if f not in bad]
    summary["files_rejected_header"] = len(bad)

    if not files:
        log.info(_j(**summary, status="NOTHING_TO_DO"))
        return summary

    raw = read_landing_csv(spark, spec, [f.uri for f in files]).persist(
        StorageLevel.MEMORY_AND_DISK)
    prepared = prepare_batch(raw, spec, batch_id)
    rejected = prepared.rejected.persist(StorageLevel.MEMORY_AND_DISK)
    valid = prepared.valid.persist(StorageLevel.MEMORY_AND_DISK)

    n_valid = valid.count()
    inserted = merge_batch(spark, spec, valid, namespace)   # commit Iceberg (atomique)
    n_rejected = rejected.count()
    if n_rejected:
        rejected.writeTo(REJECTS_TABLE).append()
    stats = file_stats(raw, rejected).collect()
    log_files(spark, batch_id, spec.name, target, files, "SUCCESS", "", stats)

    if archive and source == "landing":
        for f in files:
            landing.archive(f)

    for df in (raw, rejected, valid):
        df.unpersist()
    summary.update(files_ingested=len(files), rows_read=sum(r["rows_read"] for r in stats),
                   rows_valid=n_valid, rows_rejected=n_rejected, rows_inserted=inserted,
                   rows_already_present=max(n_valid - inserted, 0) if spec.kind == "event"
                   else None, duration_s=round(time.time() - t0, 1))
    log.info(_j(**summary, status="SUCCESS"))
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--dataset", default="all", choices=["all", "referentials", "events",
                                                         *DATASETS])
    p.add_argument("--countries", nargs="+", default=list(COUNTRIES), choices=COUNTRIES)
    p.add_argument("--source", default="landing", choices=["landing", "archive"],
                   help="archive = rejouer les fichiers déjà archivés (test d'idempotence)")
    p.add_argument("--force", action="store_true",
                   help="ignorer le journal d'ingestion (le MERGE reste idempotent)")
    p.add_argument("--no-archive", action="store_true",
                   help="laisser les fichiers dans raw-landing après ingestion")
    p.add_argument("--namespace", default=os.environ.get("INGEST_NAMESPACE", "bronze"),
                   choices=INGEST_NAMESPACES,
                   help="bronze (Level 2, défaut) ou raw (Level 1)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.dataset == "all":
        names = list(DATASETS)  # référentiels d'abord, puis événements
    elif args.dataset in ("referentials", "events"):
        kind = "referential" if args.dataset == "referentials" else "event"
        names = [n for n, s in DATASETS.items() if s.kind == kind]
    else:
        names = [args.dataset]

    spark = build_spark(f"waba-ingest-{args.namespace}-{args.dataset}")
    landing = Landing()
    ensure_tables(spark, args.namespace)
    failures = 0
    for name in names:
        try:
            ingest_dataset(spark, landing, DATASETS[name], tuple(args.countries), args.source,
                           force=args.force or args.source == "archive",
                           archive=not args.no_archive, namespace=args.namespace)
        except Exception as exc:  # un dataset en échec n'empêche pas les autres
            failures += 1
            log.exception(_j(dataset=name, status="FAILED", error=str(exc)))
    spark.stop()
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
