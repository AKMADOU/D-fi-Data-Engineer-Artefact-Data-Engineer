"""Tables Iceberg : DDL dérivé du contrat, MERGE idempotent, écritures par partition, audit."""
from __future__ import annotations

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from .schemas import DATASETS, METADATA_FIELDS, DatasetSpec

log = logging.getLogger(__name__)

TABLE_PROPERTIES = {
    "format-version": "2",
    "write.format.default": "parquet",
    "write.parquet.compression-codec": "zstd",
    # MERGE en merge-on-read : pas de réécriture des fichiers existants à chaque lot.
    "write.merge.mode": "merge-on-read",
    "write.update.mode": "merge-on-read",
    "write.delete.mode": "merge-on-read",
    "write.metadata.delete-after-commit.enabled": "true",
    "write.metadata.previous-versions-max": "50",
}

REJECTS_TABLE = "audit.rejected_records"
INGESTION_LOG_TABLE = "audit.ingestion_log"
QUARANTINE_TABLE = "audit.silver_quarantine"
NAMESPACES = ("bronze", "silver", "gold", "audit")


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------
def table_schema(spec: DatasetSpec) -> StructType:
    fields = list(spec.fields)
    if "country_code" not in {f.name for f in fields}:
        fields.append(StructField("country_code", StringType(), True))
    return StructType([*fields, *METADATA_FIELDS])


def ddl_columns(schema: StructType) -> str:
    return ",\n  ".join(f"`{f.name}` {f.dataType.simpleString()}" for f in schema.fields)


def props_sql() -> str:
    return ", ".join(f"'{k}'='{v}'" for k, v in TABLE_PROPERTIES.items())


def create_table_sql(spec: DatasetSpec, namespace: str = "raw") -> str:
    return (f"CREATE TABLE IF NOT EXISTS {spec.table_in(namespace)} (\n"
            f"  {ddl_columns(table_schema(spec))}\n)"
            f"\nUSING iceberg\nPARTITIONED BY ({', '.join(spec.partitioning_for(namespace))})"
            f"\nTBLPROPERTIES ({props_sql()})")


AUDIT_DDL = [
    f"""CREATE TABLE IF NOT EXISTS {REJECTS_TABLE} (
  batch_id string, dataset string, source_file string, record_key string,
  reject_reason string, raw_record string, rejected_at timestamp)
USING iceberg PARTITIONED BY (dataset, days(rejected_at)) TBLPROPERTIES ({props_sql()})""",
    f"""CREATE TABLE IF NOT EXISTS {INGESTION_LOG_TABLE} (
  batch_id string, dataset string, source_file string, file_etag string,
  rows_read bigint, rows_valid bigint, rows_rejected bigint, status string,
  message string, processed_at timestamp, target_table string)
USING iceberg PARTITIONED BY (dataset) TBLPROPERTIES ({props_sql()})""",
    f"""CREATE TABLE IF NOT EXISTS {QUARANTINE_TABLE} (
  dataset string, record_key string, country_code string, reason string,
  detected_at timestamp)
USING iceberg PARTITIONED BY (dataset) TBLPROPERTIES ({props_sql()})""",
]


def _ensure_column(spark: SparkSession, table: str, column: str, dtype: str) -> None:
    """Évolution de schéma : ajoute une colonne manquante (tables créées au Level 1)."""
    if column not in spark.table(table).columns:
        spark.sql(f"ALTER TABLE {table} ADD COLUMN {column} {dtype}")
        log.info("added column %s to %s", column, table)


def ensure_namespaces(spark: SparkSession) -> None:
    for ns in NAMESPACES:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")


def ensure_audit_tables(spark: SparkSession) -> None:
    ensure_namespaces(spark)
    for ddl in AUDIT_DDL:
        spark.sql(ddl)
    _ensure_column(spark, INGESTION_LOG_TABLE, "target_table", "string")


def ensure_tables(spark: SparkSession, namespace: str = "raw") -> None:
    ensure_namespaces(spark)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")  # raw : Level 1 uniquement
    for spec in DATASETS.values():
        spark.sql(create_table_sql(spec, namespace))
    ensure_audit_tables(spark)
    log.info("tables ready in %s: %s", namespace, [s.table_in(namespace)
                                                   for s in DATASETS.values()])


# ---------------------------------------------------------------------------
# Écritures
# ---------------------------------------------------------------------------
def total_records(spark: SparkSession, table: str) -> int:
    """Lignes vivantes d'après les métadonnées du dernier snapshot (sans scan).

    En merge-on-read, une mise à jour ajoute une ligne ET une position delete :
    on soustrait donc les deletes pour obtenir le nombre réel de lignes."""
    rows = spark.sql(
        f"""SELECT CAST(summary['total-records'] AS BIGINT)
                 - COALESCE(CAST(summary['total-position-deletes'] AS BIGINT), 0)
                 - COALESCE(CAST(summary['total-equality-deletes'] AS BIGINT), 0) AS n
            FROM {table}.snapshots ORDER BY committed_at DESC LIMIT 1""").collect()
    return int(rows[0]["n"]) if rows and rows[0]["n"] is not None else 0


def merge_sql(spec: DatasetSpec, source_view: str = "_batch", namespace: str = "raw") -> str:
    """MERGE idempotent sur la clé métier.

    * événements   : WHEN NOT MATCHED THEN INSERT -> rejouer un fichier n'ajoute rien ;
    * référentiels : upsert, mise à jour seulement si un attribut métier a changé
      (évite des commits inutiles quand on rejoue le même snapshot).
    """
    table = spec.table_in(namespace)
    on = f"t.`{spec.key}` = s.`{spec.key}`"
    if spec.kind == "event":
        return (f"MERGE INTO {table} t USING {source_view} s ON {on} "
                "WHEN NOT MATCHED THEN INSERT *")
    business = [f.name for f in spec.fields if f.name != spec.key]
    changed = " OR ".join(f"NOT (t.`{c}` <=> s.`{c}`)" for c in business)
    return (f"MERGE INTO {table} t USING {source_view} s ON {on} "
            f"WHEN MATCHED AND ({changed}) THEN UPDATE SET * "
            "WHEN NOT MATCHED THEN INSERT *")


def merge_batch(spark: SparkSession, spec: DatasetSpec, valid: DataFrame,
                namespace: str = "raw") -> int:
    """Exécute le MERGE (un commit Iceberg atomique) ; retourne les lignes ajoutées."""
    table = spec.table_in(namespace)
    before = total_records(spark, table)
    columns = [f.name for f in table_schema(spec).fields]
    valid.select(*columns).createOrReplaceTempView("_batch")
    spark.sql(merge_sql(spec, namespace=namespace))
    return total_records(spark, table) - before


def create_table_like_sql(schema: StructType, table: str, partitioning: tuple[str, ...],
                          comment: str = "") -> str:
    escaped = comment.replace("\\", "\\\\").replace("'", "\\'")
    comment_sql = f"\nCOMMENT '{escaped}'" if comment else ""
    return (f"CREATE TABLE IF NOT EXISTS {table} (\n  {ddl_columns(schema)}\n)"
            f"\nUSING iceberg\nPARTITIONED BY ({', '.join(partitioning)}){comment_sql}"
            f"\nTBLPROPERTIES ({props_sql()})")


def create_table_like(spark: SparkSession, df: DataFrame, table: str,
                      partitioning: tuple[str, ...], comment: str = "") -> None:
    """Crée la table Iceberg (si absente) avec le schéma du DataFrame."""
    spark.sql(create_table_like_sql(df.schema, table, partitioning, comment))


def overwrite_partitions(spark: SparkSession, df: DataFrame, table: str,
                         partitioning: tuple[str, ...], comment: str = "") -> int:
    """Écriture idempotente : remplace uniquement les partitions présentes dans `df`.

    Relancer un job pour un pays remplace les partitions de ce pays, sans
    toucher aux autres ni créer de doublons (dynamic partition overwrite)."""
    create_table_like(spark, df, table, partitioning, comment)
    target_cols = spark.table(table).columns
    missing = [c for c in df.columns if c not in target_cols]
    for col in missing:  # évolution de schéma additive
        dtype = df.schema[col].dataType.simpleString()
        spark.sql(f"ALTER TABLE {table} ADD COLUMN `{col}` {dtype}")
    target = spark.table(table)
    # Colonnes présentes en table mais plus produites (renommage) : NULL, pas d'échec.
    df = df.select(*[F.col(c) if c in df.columns else
                     F.lit(None).cast(target.schema[c].dataType).alias(c)
                     for c in target.columns])
    n = df.count()
    df.writeTo(table).overwritePartitions()
    log.info("wrote %d rows to %s", n, table)
    return n
