"""Destinations d'écriture : MinIO (S3) ou système de fichiers local (tests).

Organisation du bucket raw-landing (« sous-dossiers pays/type ») :
    raw-landing/<CC>/<dataset>/<prefix>_<CC>_<YYYYMMDD>_<NN>.csv
    raw-landing/referentials/<name>/<name>.csv            (snapshot complet)
    raw-landing/referentials/<name>/<name>_delta_<YYYYMMDD>_<NN>.csv
"""
from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Protocol

log = logging.getLogger(__name__)

_SEQ_RE = re.compile(r"_(\d{8})_(\d+)\.csv$")


class Sink(Protocol):
    """Interface minimale d'une destination (permet de tester sans MinIO)."""

    def put(self, key: str, data: bytes) -> None: ...

    def list_keys(self, prefix: str) -> list[str]: ...

    def get(self, key: str) -> bytes: ...


class S3Sink:
    """Écrit dans un bucket MinIO via l'API S3 (boto3).

    Les identifiants sont lus dans l'environnement : aucun secret dans le code.
    `extra_list_buckets` permet de tenir compte des fichiers déjà archivés lors
    du calcul du numéro de séquence NN (évite d'écraser un nom déjà utilisé).
    """

    def __init__(self, bucket: str, extra_list_buckets: list[str] | None = None) -> None:
        import boto3  # import local : boto3 inutile pour les tests locaux
        from botocore.config import Config

        self.bucket = bucket
        self.extra_list_buckets = extra_list_buckets or []
        self.client = boto3.client(
            "s3",
            endpoint_url=os.environ["S3_ENDPOINT"],
            aws_access_key_id=os.environ["S3_ACCESS_KEY"],
            aws_secret_access_key=os.environ["S3_SECRET_KEY"],
            region_name=os.environ.get("AWS_REGION", "us-east-1"),
            config=Config(s3={"addressing_style": "path"}, retries={"max_attempts": 5}),
        )

    def put(self, key: str, data: bytes) -> None:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=data,
                               ContentType="text/csv")
        log.info("uploaded s3://%s/%s (%d bytes)", self.bucket, key, len(data))

    def get(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def list_keys(self, prefix: str) -> list[str]:
        keys: list[str] = []
        paginator = self.client.get_paginator("list_objects_v2")
        for bucket in [self.bucket, *self.extra_list_buckets]:
            for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
                keys.extend(obj["Key"] for obj in page.get("Contents", []))
        return keys


class LocalSink:
    """Écrit dans un répertoire local (mêmes clés que le bucket)."""

    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)

    def put(self, key: str, data: bytes) -> None:
        path = self.root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    def get(self, key: str) -> bytes:
        return (self.root / key).read_bytes()

    def list_keys(self, prefix: str) -> list[str]:
        base = self.root / prefix
        folder = base if base.is_dir() else base.parent
        if not folder.exists():
            return []
        return [str(p.relative_to(self.root)) for p in folder.rglob("*")
                if p.is_file() and str(p.relative_to(self.root)).startswith(prefix)]


def next_sequences(sink: Sink, prefix: str) -> dict[str, int]:
    """Retourne, pour chaque date YYYYMMDD déjà présente sous `prefix`, le dernier NN."""
    last: dict[str, int] = {}
    for key in sink.list_keys(prefix):
        m = _SEQ_RE.search(key)
        if m:
            day, seq = m.group(1), int(m.group(2))
            last[day] = max(last.get(day, 0), seq)
    return last
