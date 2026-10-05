"""Accès au bucket raw-landing : inventaire des fichiers, contrôle d'en-tête, archivage."""
from __future__ import annotations

import logging
from dataclasses import dataclass

import boto3
from botocore.config import Config

from .schemas import DatasetSpec
from .session import env

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class LandingFile:
    bucket: str
    key: str
    etag: str
    size: int

    @property
    def uri(self) -> str:
        """URI lue par Spark (identique à input_file_name())."""
        return f"s3a://{self.bucket}/{self.key}"


class Landing:
    def __init__(self) -> None:
        self.landing_bucket = env("LANDING_BUCKET", "raw-landing")
        self.archive_bucket = env("ARCHIVE_BUCKET", "archive")
        self.s3 = boto3.client(
            "s3", endpoint_url=env("S3_ENDPOINT"),
            aws_access_key_id=env("S3_ACCESS_KEY"),
            aws_secret_access_key=env("S3_SECRET_KEY"),
            region_name=env("AWS_REGION", "us-east-1"),
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}))

    def list_files(self, spec: DatasetSpec, countries: tuple[str, ...],
                   bucket: str | None = None) -> list[LandingFile]:
        bucket = bucket or self.landing_bucket
        files: list[LandingFile] = []
        paginator = self.s3.get_paginator("list_objects_v2")
        for prefix in spec.landing_prefixes(countries):
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith(".csv") and obj["Size"] > 0:
                        files.append(LandingFile(bucket, obj["Key"],
                                                 obj["ETag"].strip('"'), obj["Size"]))
        return sorted(files, key=lambda f: f.key)

    def header_matches(self, f: LandingFile, spec: DatasetSpec) -> bool:
        """Contrôle de contrat : l'en-tête doit lister les colonnes attendues, dans l'ordre.
        (La lecture Spark est positionnelle : un fichier aux colonnes permutées serait
        sinon chargé silencieusement dans les mauvaises colonnes.)"""
        head = self.s3.get_object(Bucket=f.bucket, Key=f.key, Range="bytes=0-4095")
        first_line = head["Body"].read().decode("utf-8", "replace").splitlines()[0]
        cols = [c.strip().strip('"') for c in first_line.lstrip("\ufeff").split(",")]
        return cols == [fld.name for fld in spec.fields]

    def archive(self, f: LandingFile, sub_prefix: str = "") -> None:
        """Déplace le fichier (copie puis suppression) vers le bucket archive.

        Appelé uniquement APRÈS le commit Iceberg : si l'archivage échoue, le
        fichier sera relu au prochain run et le MERGE empêchera tout doublon.
        """
        dest = f"{sub_prefix}{f.key}"
        self.s3.copy_object(Bucket=self.archive_bucket, Key=dest,
                            CopySource={"Bucket": f.bucket, "Key": f.key})
        self.s3.delete_object(Bucket=f.bucket, Key=f.key)
        log.info("archived s3://%s/%s -> s3://%s/%s", f.bucket, f.key,
                 self.archive_bucket, dest)
