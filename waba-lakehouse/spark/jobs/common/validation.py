"""Validation et préparation d'un lot (fonctions pures, testables sans Iceberg ni MinIO).

Toutes les règles sont des expressions Spark natives (pas d'UDF Python) : elles
s'exécutent dans la JVM et n'ont pas besoin d'être distribuées aux workers.
"""
from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from .schemas import (COUNTRIES, CORRUPT_COL, CURRENCIES, ENTITY_TYPES, UEMOA,
                      DatasetSpec)

REASON_COL = "reject_reason"


def expected_currency(country: Column) -> Column:
    """XOF pour la zone UEMOA, GHS pour le Ghana (NULL si pays inconnu)."""
    return F.when(country.isin(*UEMOA), F.lit("XOF")).when(country == "GH", F.lit("GHS"))


def reject_reasons(spec: DatasetSpec) -> Column:
    """Concatène les motifs de rejet ; chaîne vide = ligne valide."""
    checks: list[Column] = [
        F.when(F.col(CORRUPT_COL).isNotNull(), F.lit("MALFORMED_ROW")),
    ]
    checks += [F.when(F.col(c).isNull(), F.lit(f"MISSING_{c.upper()}"))
               for c in spec.required]
    country = F.col(spec.country_col)
    checks.append(F.when(~country.isin(*COUNTRIES), F.lit("INVALID_COUNTRY")))
    checks.append(F.when(~F.col("entity_type").isin(*ENTITY_TYPES), F.lit("INVALID_ENTITY")))
    if spec.currency_col:
        cur = F.col(spec.currency_col)
        checks.append(F.when(~cur.isin(*CURRENCIES), F.lit("INVALID_CURRENCY")))
        checks.append(F.when(cur.isin(*CURRENCIES) & (cur != expected_currency(country)),
                             F.lit("CURRENCY_COUNTRY_MISMATCH")))
    checks += [F.when(F.col(c).isNotNull() & ~F.col(c).isin(*values),
                      F.lit(f"INVALID_{c.upper()}"))
               for c, values in spec.enums.items()]
    checks += [F.when(F.col(c) < 0, F.lit(f"NEGATIVE_{c.upper()}"))
               for c in spec.non_negative]
    if spec.key_regex:
        checks.append(F.when(F.col(spec.key).isNotNull() & ~F.col(spec.key).rlike(spec.key_regex),
                             F.lit("INVALID_KEY_FORMAT")))
    # concat_ws ignore les NULL : seules les règles en échec apparaissent.
    return F.concat_ws(";", *checks)


def read_landing_csv(spark: SparkSession, spec: DatasetSpec, paths: list[str]) -> DataFrame:
    """Lecture CSV avec schéma explicite ; les lignes illisibles sont conservées
    dans `_corrupt_record` (mode PERMISSIVE) pour être rejetées, pas perdues."""
    return (spark.read
            .option("header", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", CORRUPT_COL)
            .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss[.SSS]XXX")
            .option("dateFormat", "yyyy-MM-dd")
            .schema(spec.read_schema)
            .csv(paths)
            .withColumn("_source_file", F.input_file_name()))


@dataclass
class PreparedBatch:
    valid: DataFrame      # lignes à fusionner dans raw.<table>
    rejected: DataFrame   # lignes rejetées (+ motif) pour audit.rejected_records


def prepare_batch(df: DataFrame, spec: DatasetSpec, batch_id: str) -> PreparedBatch:
    """Valide, déduplique au sein du lot et ajoute les colonnes techniques.

    * lignes invalides -> `rejected` avec le motif ;
    * doublons de clé dans le lot -> une seule ligne conservée, les autres rejetées
      (DUPLICATE_IN_BATCH). Pour un référentiel, le fichier le plus récent gagne.
    """
    checked = df.withColumn(REASON_COL, reject_reasons(spec))
    invalid = checked.filter(F.col(REASON_COL) != "")
    candidates = checked.filter(F.col(REASON_COL) == "")

    order = [F.col("_source_file").desc()] if spec.kind == "referential" \
        else [F.col("_source_file").asc()]
    w = Window.partitionBy(spec.key).orderBy(*order)
    ranked = candidates.withColumn("_rn", F.row_number().over(w))
    duplicates = (ranked.filter("_rn > 1").drop("_rn")
                  .withColumn(REASON_COL, F.lit("DUPLICATE_IN_BATCH")))

    now = F.current_timestamp()
    valid = (ranked.filter("_rn = 1")
             .drop("_rn", REASON_COL, CORRUPT_COL)
             .withColumn("_batch_id", F.lit(batch_id))
             .withColumn("_ingested_at", now))
    if "country_code" not in valid.columns:
        # Mobile money : country_code (exigé sur toutes les tables) = pays émetteur.
        valid = valid.withColumn("country_code", F.col(spec.country_col))

    payload_cols = [f.name for f in spec.fields]
    rejected = (invalid.unionByName(duplicates)
                .select(
                    F.lit(batch_id).alias("batch_id"),
                    F.lit(spec.name).alias("dataset"),
                    F.col("_source_file").alias("source_file"),
                    F.col(spec.key).alias("record_key"),
                    F.col(REASON_COL).alias("reject_reason"),
                    # Ligne brute si illisible, sinon les champs parsés en JSON.
                    F.coalesce(F.col(CORRUPT_COL),
                               F.to_json(F.struct(*payload_cols))).alias("raw_record"),
                    now.alias("rejected_at")))
    return PreparedBatch(valid=valid, rejected=rejected)


def file_stats(df: DataFrame, rejected: DataFrame) -> DataFrame:
    """Comptages par fichier source (lu / rejeté) pour le journal d'ingestion."""
    read = df.groupBy("_source_file").agg(F.count("*").alias("rows_read"))
    rej = (rejected.groupBy(F.col("source_file").alias("_source_file"))
           .agg(F.count("*").alias("rows_rejected")))
    return (read.join(rej, "_source_file", "left")
            .fillna(0, ["rows_rejected"])
            .withColumn("rows_valid", F.col("rows_read") - F.col("rows_rejected")))
