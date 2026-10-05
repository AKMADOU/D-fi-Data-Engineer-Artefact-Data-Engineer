"""Découpage en fichiers selon la nomenclature de l'énoncé et dépôt dans raw-landing."""
from __future__ import annotations

import io
import logging
from datetime import date

import pandas as pd

from . import config as C
from .storage import Sink, next_sequences
from .transactions import COUNTRY_COLUMN

log = logging.getLogger(__name__)


def to_csv_bytes(df: pd.DataFrame, columns: list[str]) -> bytes:
    """CSV UTF-8, en-tête, booléens en minuscules, NULL = champ vide."""
    out = df[columns].copy()
    for col in out.columns:
        if out[col].dtype == bool:
            out[col] = out[col].map({True: "true", False: "false"})
    buf = io.StringIO()
    out.to_csv(buf, index=False, date_format="%Y-%m-%d")
    return buf.getvalue().encode("utf-8")


def event_key(dataset: str, country: str, day: str, seq: int) -> str:
    """Ex. CI/bank_transactions/bank_txn_CI_20260101_01.csv"""
    prefix = C.EVENT_DATASETS[dataset]
    return f"{country}/{dataset}/{prefix}_{country}_{day}_{seq:02d}.csv"


def write_events(df: pd.DataFrame, dataset: str, sink: Sink) -> list[str]:
    """Un fichier par (pays, jour). NN = séquence suivante pour ne jamais écraser."""
    country_col = COUNTRY_COLUMN.get(dataset, "country_code")
    keys: list[str] = []
    for cc, per_country in df.groupby(country_col, sort=True):
        seqs = next_sequences(sink, f"{cc}/{dataset}/")
        for day, chunk in per_country.groupby("_file_date", sort=True):
            seq = seqs.get(day, 0) + 1
            seqs[day] = seq
            key = event_key(dataset, cc, day, seq)
            sink.put(key, to_csv_bytes(chunk, C.COLUMNS[dataset]))
            keys.append(key)
    log.info("%s: %d files written", dataset, len(keys))
    return keys


def write_referentials(frames: dict[str, pd.DataFrame], sink: Sink,
                       delta_day: date | None = None) -> list[str]:
    """Snapshot complet (`customers.csv`) ou delta (`customers_delta_YYYYMMDD_NN.csv`)."""
    keys = []
    for name, df in frames.items():
        folder = f"referentials/{name}/"
        if delta_day is None:
            key = f"{folder}{name}.csv"
        else:
            day = delta_day.strftime("%Y%m%d")
            seq = next_sequences(sink, folder).get(day, 0) + 1
            key = f"{folder}{name}_delta_{day}_{seq:02d}.csv"
        sink.put(key, to_csv_bytes(df, C.COLUMNS[name]))
        keys.append(key)
    return keys
