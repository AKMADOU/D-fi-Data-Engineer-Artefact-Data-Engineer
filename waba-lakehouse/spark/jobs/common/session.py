"""Construction de la SparkSession (Iceberg REST + MinIO) à partir de l'environnement.

Aucun secret dans le code ni dans spark-defaults.conf : tout vient des variables
d'environnement injectées par docker compose depuis le fichier .env.
Spark masque automatiquement les clés *secret* dans l'UI (spark.redaction.regex).
"""
from __future__ import annotations

import os

from pyspark.sql import SparkSession


def env(name: str, default: str | None = None) -> str:
    value = os.environ.get(name, default)
    if value is None:
        raise RuntimeError(f"Variable d'environnement manquante : {name}")
    return value


def build_spark(app_name: str) -> SparkSession:
    endpoint = env("S3_ENDPOINT")
    access_key, secret_key = env("S3_ACCESS_KEY"), env("S3_SECRET_KEY")
    region = env("AWS_REGION", "us-east-1")
    cat = "spark.sql.catalog.iceberg"
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # Catalogue Iceberg REST (partagé avec Trino) ; données dans s3://lakehouse
        .config(cat, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{cat}.type", "rest")
        .config(f"{cat}.uri", env("ICEBERG_REST_URI", "http://iceberg-rest:8181"))
        .config(f"{cat}.warehouse", env("ICEBERG_WAREHOUSE", "s3://lakehouse/"))
        .config(f"{cat}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{cat}.s3.endpoint", endpoint)
        .config(f"{cat}.s3.path-style-access", "true")
        .config(f"{cat}.s3.access-key-id", access_key)
        .config(f"{cat}.s3.secret-access-key", secret_key)
        .config(f"{cat}.client.region", region)
        .config("spark.sql.defaultCatalog", "iceberg")
        # Lecture des CSV de raw-landing via s3a
        .config("spark.hadoop.fs.s3a.endpoint", endpoint)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled",
                str(endpoint.startswith("https")).lower())
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.session.timeZone", "UTC")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
