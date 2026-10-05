"""Couche service partagée par l'application Streamlit et la CLI."""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from . import config as C
from .referentials import ReferentialStore
from .storage import LocalSink, S3Sink, Sink
from .scenarios import generate_scenarios
from .transactions import generate_events
from .writer import write_events, write_referentials

log = logging.getLogger(__name__)

STATE_DIR = os.environ.get("GENERATOR_STATE_DIR", "/data/state")
LANDING_BUCKET = os.environ.get("LANDING_BUCKET", "raw-landing")
ARCHIVE_BUCKET = os.environ.get("ARCHIVE_BUCKET", "archive")


def get_sink() -> Sink:
    """MinIO par défaut ; `LOCAL_OUTPUT_DIR` permet un mode hors-ligne (tests)."""
    local_dir = os.environ.get("LOCAL_OUTPUT_DIR")
    if local_dir:
        return LocalSink(local_dir)
    # Les fichiers déjà archivés comptent pour la numérotation NN.
    return S3Sink(LANDING_BUCKET, extra_list_buckets=[ARCHIVE_BUCKET])


def load_store() -> ReferentialStore:
    return ReferentialStore.load(STATE_DIR)


@dataclass
class RunResult:
    dataset: str
    rows: int
    files: list[str] = field(default_factory=list)
    anomalies: dict[str, int] = field(default_factory=dict)


def generate_referentials(sink: Sink, sizes: dict[str, int] | None = None, seed: int = 42,
                          countries: list[str] | None = None,
                          entities: list[str] | None = None) -> tuple[ReferentialStore, RunResult]:
    store = ReferentialStore.generate(sizes, seed=seed, countries=countries, entities=entities)
    files = write_referentials(store.frames(), sink)
    store.save(STATE_DIR)
    rows = sum(len(df) for df in store.frames().values())
    return store, RunResult("referentials", rows, files)


def generate_referential_delta(store: ReferentialStore, sink: Sink, n_customers: int,
                               countries: list[str] | None = None,
                               entities: list[str] | None = None) -> RunResult:
    today = datetime.now(timezone.utc).date()
    delta = store.add_delta(n_customers, countries=countries, entities=entities,
                            onboarding_day=today)
    files = write_referentials(delta, sink, delta_day=today)
    store.save(STATE_DIR)
    return RunResult("referentials_delta", sum(len(d) for d in delta.values()), files)


def generate_dataset(dataset: str, store: ReferentialStore, sink: Sink, n_rows: int,
                     countries: list[str], entities: list[str], start: datetime,
                     end: datetime, seed: int | None = None,
                     anomaly_rate: float = 0.0) -> RunResult:
    df, anomalies = generate_events(dataset, store, n_rows, countries, entities, start, end,
                                    seed=seed, anomaly_rate=anomaly_rate)
    files = write_events(df, dataset, sink)
    return RunResult(dataset, len(df), files, anomalies)


def continuous_tick(dataset: str, store: ReferentialStore, sink: Sink, n_rows: int,
                    countries: list[str], entities: list[str], window_seconds: int,
                    anomaly_rate: float = 0.0) -> RunResult:
    """Un micro-lot horodaté sur la fenêtre [now - window, now] (flux « temps réel »)."""
    if dataset == "referentials":
        return generate_referential_delta(store, sink, n_rows, countries, entities)
    end = datetime.now(timezone.utc).replace(tzinfo=None)
    start = end - timedelta(seconds=window_seconds)
    return generate_dataset(dataset, store, sink, n_rows, countries, entities, start, end,
                            anomaly_rate=anomaly_rate)


def generate_fraud_scenarios(store: ReferentialStore, sink: Sink, names: list[str],
                             countries: list[str]) -> list[RunResult]:
    """Level 3 : dépose des motifs de fraude / AML / liquidité horodatés maintenant."""
    results = []
    for dataset, df in generate_scenarios(store, names, countries).items():
        results.append(RunResult(dataset, len(df), write_events(df, dataset, sink)))
    return results


ALL_DATASETS = ["referentials", *C.EVENT_DATASETS]
