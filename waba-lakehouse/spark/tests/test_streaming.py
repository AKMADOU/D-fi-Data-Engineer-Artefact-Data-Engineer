"""Tests Level 3 : logique temps réel sans Kafka (sources fichiers / DataFrames « façon Kafka »).

* parsing JSON NiFi + règles de qualité du L1 + DLQ ;
* déduplication 10 min en vrai Structured Streaming (source fichiers, sink mémoire) ;
* fenêtre glissante de fraude en vrai streaming (état entre micro-lots) ;
* bout en bout : scénarios du générateur -> Job 1 -> JSON silver-* -> Job 2 -> alertes.
"""
from __future__ import annotations

import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "spark" / "jobs"))
sys.path.insert(0, str(ROOT / "generator"))

from common import streaming as S  # noqa: E402
from common.fx import fx_rates_df  # noqa: E402
from common.lakehouse import table_schema  # noqa: E402
from common.schemas import DATASETS  # noqa: E402
from common.silver_transforms import silver_accounts  # noqa: E402
from common.stream_pipeline import (LookupCache, MemorySinks, dedupe_alerts,  # noqa: E402
                                    process_event_batch, process_raw_batch)

SECRET = "test-secret"
KAFKA_SCHEMA = "value string, topic string, partition int, offset long, timestamp timestamp"


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("streaming-tests")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s


# --------------------------------------------------------------------------- helpers
def nifi_json(row: dict, source: str) -> str:
    """Ce que produit le flux NiFi : CSV -> JSON (champs texte) + métadonnées d'ingestion."""
    out = {k: ("" if v is None or (isinstance(v, float) and v != v) else str(v))
           for k, v in row.items() if not k.startswith("_")}
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    out.update(ingestion_timestamp=now, source_file=source, landed_at=now)
    return json.dumps(out)


def kafka_df(spark, messages: list[tuple[str, str]], ts: datetime | None = None):
    ts = ts or datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [(v, t, 0, i, ts) for i, (t, v) in enumerate(messages)]
    return (spark.createDataFrame(rows, KAFKA_SCHEMA)
            .withColumn("kafka_ts", F.col("timestamp")).drop("timestamp"))


def bronze_frame(spark, name: str, pdf):
    """Référentiel du générateur -> DataFrame au schéma Bronze (colonnes typées)."""
    spec = DATASETS[name]
    str_df = spark.createDataFrame(pdf.astype(str).replace({"nan": None, "None": None}))
    cols = [F.col(f.name).cast(f.dataType).alias(f.name) for f in spec.fields]
    return str_df.select(*cols, F.lit("s3a://raw-landing/ref.csv").alias("_source_file"),
                         F.lit("b").alias("_batch_id"),
                         F.current_timestamp().alias("_ingested_at"))


# --------------------------------------------------------------------------- unitaires
def test_event_key_and_parse_raw(spark):
    good = {"transaction_id": "a" * 8 + "-aaaa-4aaa-8aaa-" + "a" * 12,
            "timestamp": "2026-09-29T10:00:00Z", "account_id": "WABA-CI-A-0000001",
            "beneficiary_account": "", "branch_id": "WABA-CI-B-001", "country_code": "CI",
            "transaction_type": "WITHDRAWAL", "amount": "15000.00", "currency": "XOF",
            "channel": "ATM", "transaction_status": "SUCCESS", "fee_amount": "15",
            "entity_type": "BANK"}
    bad_cur = {**good, "transaction_id": good["transaction_id"][:-1] + "b", "currency": "GHS"}
    bad_amount = {**good, "transaction_id": good["transaction_id"][:-1] + "c", "amount": "abc"}
    t = S.RAW_TOPICS["bank_transactions"]
    msgs = kafka_df(spark, [(t, nifi_json(good, "CI/f.csv")), (t, nifi_json(bad_cur, "x")),
                            (t, nifi_json(bad_amount, "x")), (t, "{not json")])
    valid, dlq = S.parse_raw(msgs, DATASETS["bank_transactions"], 1)
    v = valid.collect()
    assert len(v) == 1 and float(v[0]["amount"]) == 15000.0
    assert v[0]["beneficiary_account"] is None                      # "" -> NULL
    assert v[0]["_source_file"] == f"kafka://{t}/0/0" and v[0]["_landing_file"] == "CI/f.csv"
    reasons = sorted(r["reject_reason"].split(";")[0] for r in dlq.collect())
    assert reasons == ["CURRENCY_COUNTRY_MISMATCH", "MALFORMED_ROW", "MALFORMED_ROW"]
    # la DLQ transporte le message d'origine
    out = [json.loads(r["value"]) for r in S.dlq_records(dlq, "bank_transactions", "raw").collect()]
    assert {o["stage"] for o in out} == {"raw"} and any(o["raw_record"] == "{not json" for o in out)
    keys = msgs.select(S.event_key(F.col("value"), F.col("topic"), F.col("partition"),
                                   F.col("offset")).alias("k")).collect()
    assert keys[0]["k"] == good["transaction_id"] and keys[3]["k"] == f"{t}:0:3"


def test_to_kafka_json_roundtrip(spark):
    df = spark.createDataFrame([("T1", datetime(2026, 9, 29, 10, 0, 1, 250000), 12.5)],
                               "transaction_id string, timestamp timestamp, amount_eur double")
    row = S.to_kafka_json(df, "transaction_id").first()
    assert row["key"] == "T1"
    assert json.loads(row["value"])["timestamp"] == "2026-09-29T10:00:01.250"


def test_alert_rules_static(spark):
    base = datetime(2026, 9, 29, 10, 0)
    big = S.LARGE_TXN_EUR + 100
    bank = spark.createDataFrame([
        ("T1", base, "CI", "BANK", "TRANSFER", "SUCCESS", 800000.0, "XOF", big, "K1", "C1", True),
        ("T2", base + timedelta(seconds=90), "CI", "BANK", "TRANSFER", "SUCCESS", 900000.0,
         "XOF", big, "K1", "C1", True),
        ("T3", base + timedelta(minutes=30), "CI", "BANK", "TRANSFER", "SUCCESS", 800000.0,
         "XOF", big, "K1", "C1", True),                     # isolée : pas de rafale
        ("T4", base, "GH", "BANK", "INTERNATIONAL_WIRE", "SUCCESS", 6000.0, "GHS", 380.0,
         "K2", "C2", True),                                 # > 5 000 GHS -> AML
        ("T5", base, "CI", "BANK", "TRANSFER", "SUCCESS", 999999.0, "XOF", 1524.0, "K3", "C3",
         True)],                                           # juste sous 1 M XOF
        ", ".join(f"{n} {t}" for n, t in S.SILVER_JSON_SCHEMAS["bank_transactions"])
        .replace("timestamp string", "timestamp timestamp"))
    bursts = S.large_txn_bursts(bank).collect()
    assert bursts and {b["subject_key"] for b in bursts} == {"K1"}
    assert len({b["alert_id"] for b in bursts}) == 1        # fenêtres glissantes -> 1 alerte
    assert json.loads(bursts[0]["details"])["nb_transactions"] == 2
    aml = S.aml_events(bank, bank.limit(0).select(
        F.lit("P").alias("payment_id"), "timestamp", "country_code",
        F.lit("P2P").alias("payment_type"), "amount", "currency", "amount_eur",
        F.lit("S").alias("sender_key"))).collect()
    assert [a["subject_key"] for a in aml] == ["K2"] and aml[0]["threshold"] == 5000.0


def test_suppress_repeats(spark):
    cols = "alert_id string, rule string, subject_key string, event_time timestamp, amount_eur double"
    t = datetime(2026, 9, 29, 10, 0)
    new = spark.createDataFrame([("new", "R", "K", t + timedelta(minutes=2), 1.0),
                                 ("same", "R", "K2", t, 1.0),
                                 ("late", "R", "K", t + timedelta(minutes=20), 1.0)], cols)
    recent = spark.createDataFrame([("old", "R", "K", t, 1.0), ("same", "R", "K2", t, 1.0)], cols)
    kept = {r["alert_id"] for r in S.suppress_repeats(new, recent).collect()}
    assert kept == {"same", "late"}   # "same" = mise à jour de l'alerte existante


# --------------------------------------------------------------------------- vrai streaming
def _write_json(path: Path, name: str, rows: list[dict]):
    path.mkdir(parents=True, exist_ok=True)
    (path / name).write_text("\n".join(json.dumps(r) for r in rows))


def test_streaming_dedup_within_10_minutes(spark, tmp_path):
    src = tmp_path / "src"
    t = S.RAW_TOPICS["bank_transactions"]
    now = datetime(2026, 9, 29, 10, 0)

    def msg(tid, minutes, off):
        return {"value": json.dumps({"transaction_id": tid}), "topic": t, "partition": 0,
                "offset": off, "timestamp": (now + timedelta(minutes=minutes)).isoformat()}
    _write_json(src, "1.json", [msg("A", 0, 0), msg("A", 1, 1), msg("B", 1, 2)])
    stream = spark.readStream.schema(KAFKA_SCHEMA).option("maxFilesPerTrigger", 1).json(str(src))
    q = (S.dedup_stream(stream).writeStream.format("memory").queryName("dedup_out")
         .option("checkpointLocation", str(tmp_path / "ck")).start())
    q.processAllAvailable()
    _write_json(src, "2.json", [msg("A", 5, 3),          # rejeu à +5 min : doublon
                                msg("C", 30, 4)])        # fait avancer le watermark
    q.processAllAvailable()
    _write_json(src, "3.json", [msg("A", 31, 5)])        # > 10 min après : nouvel événement
    q.processAllAvailable()
    q.stop()
    keys = [r["event_key"] for r in spark.table("dedup_out").orderBy("offset").collect()]
    assert keys == ["A", "B", "C", "A"]


def test_streaming_sliding_window_bursts(spark, tmp_path):
    """La rafale est répartie sur deux micro-lots : l'état de la fenêtre doit la reconstituer."""
    src = tmp_path / "src"
    fields = S.SILVER_JSON_SCHEMAS["bank_transactions"]
    schema = ", ".join(f"{n} {t}" for n, t in fields).replace("timestamp string",
                                                              "timestamp timestamp")
    base = datetime(2026, 9, 29, 10, 0)

    def txn(tid, sec, acc="K1"):
        return {"transaction_id": tid, "timestamp": (base + timedelta(seconds=sec)).isoformat(),
                "country_code": "CI", "entity_type": "BANK", "transaction_type": "TRANSFER",
                "transaction_status": "SUCCESS", "amount": 800000.0, "currency": "XOF",
                "amount_eur": S.LARGE_TXN_EUR + 1, "account_key": acc, "customer_key": "C",
                "is_success": True}
    _write_json(src, "1.json", [txn("T1", 0), txn("X1", 10, "K9")])
    stream = (spark.readStream.schema(schema).option("maxFilesPerTrigger", 1).json(str(src))
              .withWatermark("timestamp", "10 minutes"))
    sinks = MemorySinks()
    from common.stream_pipeline import emit_alerts
    q = (S.large_txn_bursts(stream).writeStream.outputMode("update")
         .foreachBatch(lambda df, _: emit_alerts(df, "fraud", sinks))
         .option("checkpointLocation", str(tmp_path / "ck")).start())
    q.processAllAvailable()
    assert not sinks.tables                          # une seule grosse transaction : rien
    _write_json(src, "2.json", [txn("T2", 150)])     # 2e transaction 2 min 30 plus tard
    q.processAllAvailable()
    q.stop()
    alerts = list(sinks.tables["gold.fraud_alerts"].values())
    assert len(alerts) == 1 and alerts[0]["subject_key"] == "K1"
    assert set(json.loads(alerts[0]["details"])["transaction_ids"]) == {"T1", "T2"}
    assert len(sinks.topics[S.FRAUD_TOPIC]) == 1


# --------------------------------------------------------------------------- bout en bout
@pytest.fixture(scope="module")
def world(spark):
    pytest.importorskip("waba_gen", reason="générateur absent (tests e2e lancés depuis le dépôt)")
    from waba_gen.referentials import ReferentialStore
    from waba_gen.scenarios import SCENARIOS, generate_scenarios
    store = ReferentialStore.generate(
        {"customers": 3_000, "accounts": 5_000, "branches": 100, "products": 50}, seed=3)
    lookups = {n: bronze_frame(spark, n, getattr(store, n)).cache()
               for n in ("customers", "accounts", "branches", "products")}
    lookups["fx"] = fx_rates_df(spark).cache()
    events = generate_scenarios(store, list(SCENARIOS), ["CI", "GH"], seed=5)
    messages = []
    for ds, pdf in events.items():
        for rec in pdf.to_dict("records"):
            messages.append((S.RAW_TOPICS[ds], nifi_json(rec, f"landing/{ds}.csv")))
    # un message corrompu et un compte inconnu : ils doivent finir en DLQ
    messages.append((S.RAW_TOPICS["bank_transactions"], '{"transaction_id": "oops"'))
    orphan = dict(events["bank_transactions"].iloc[0])
    orphan.update(transaction_id="0" * 8 + "-0000-4000-8000-" + "0" * 12,
                  account_id="WABA-CI-A-9999999")
    messages.append((S.RAW_TOPICS["bank_transactions"], nifi_json(orphan, "landing/x.csv")))
    sinks = MemorySinks()
    rep = process_raw_batch(kafka_df(spark, messages), 0, LookupCache(lambda: lookups), sinks,
                            SECRET, audit_table=None)
    return {"store": store, "lookups": lookups, "events": events, "sinks": sinks, "rep": rep}


def test_job1_writes_silver_rt_and_dlq(world):
    rep, sinks, events = world["rep"], world["sinks"], world["events"]
    assert rep.valid["bank_transactions"] == len(events["bank_transactions"])
    assert rep.valid["mobile_money_payments"] == len(events["mobile_money_payments"])
    assert set(sinks.tables) == {"silver.rt_bank_transactions",
                                 "silver.rt_insurance_operations",
                                 "silver.rt_mobile_money_payments"}
    assert len(sinks.topics[S.SILVER_TOPICS["bank_transactions"]]) == rep.valid["bank_transactions"]
    dlq = [json.loads(m["value"]) for m in sinks.topics[S.DLQ_TOPIC]]
    assert {d["reject_reason"].split(";")[0] for d in dlq} == {"MALFORMED_ROW", "ORPHAN_ACCOUNT"}
    assert rep.max_lag_s is not None and rep.max_lag_s < 30
    # pseudonymisation : aucun identifiant réel dans les topics silver
    payload = " ".join(m["value"] for m in sinks.topics[S.SILVER_TOPICS["bank_transactions"]])
    assert "WABA-CI-A-" not in payload


def _silver_stream_df(spark, sinks, dataset):
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [(m["value"], now, S.SILVER_TOPICS[dataset]) for m in sinks.topics[S.SILVER_TOPICS[dataset]]]
    return S.parse_silver(spark.createDataFrame(rows, "value string, timestamp timestamp, "
                                                      "topic string"), dataset)


def test_job2_raises_every_scenario_alert(spark, world):
    sinks1, lk = world["sinks"], world["lookups"]
    bank = _silver_stream_df(spark, sinks1, "bank_transactions").cache()
    mm = _silver_stream_df(spark, sinks1, "mobile_money_payments")
    ins = _silver_stream_df(spark, sinks1, "insurance_operations")
    empty_hist = ins.limit(0).select("operation_id", "operation_type", "timestamp",
                                     "amount_eur", "account_key")
    out = MemorySinks()
    counts = process_event_batch(bank, mm, ins, empty_hist, out)
    from common.stream_pipeline import emit_alerts
    emit_alerts(S.large_txn_bursts(bank), "fraud", out)
    reserves = S.liquidity_reserves(silver_accounts(lk["accounts"], lk["customers"],
                                                    lk["products"], lk["fx"], SECRET)[0])
    emit_alerts(S.liquidity_alerts(S.liquidity_flows(bank), reserves), "liquidity", out)

    fraud = list(out.tables["gold.fraud_alerts"].values())
    rules = {a["rule"] for a in fraud}
    assert rules == {"MULTIPLE_LARGE_TXN", "UNUSUAL_COUNTRY", "CLAIM_EXCEEDS_PREMIUM"}
    assert counts["aml"] >= 2
    aml = list(out.tables["gold.aml_events"].values())
    assert {a["currency"] for a in aml} == {"XOF", "GHS"}
    liq = list(out.tables["gold.liquidity_alerts"].values())
    assert {a["country_code"] for a in liq} == {"CI", "GH"}
    assert all(json.loads(a["details"])["share_of_reserve"] > S.LIQUIDITY_ALERT_SHARE for a in liq)
    # chaque alerte est aussi publiée dans son topic Kafka
    assert len(out.topics[S.FRAUD_TOPIC]) >= len(fraud)
    # rejouer le même micro-lot ne crée pas de nouvelles alertes (MERGE sur alert_id)
    before = {k: len(v) for k, v in out.tables.items()}
    process_event_batch(bank, mm, ins, empty_hist, out)
    assert {k: len(v) for k, v in out.tables.items()} == before


def test_dedupe_alerts_one_per_subject(spark):
    cols = "alert_id string, rule string, subject_key string, event_time timestamp, amount_eur double"
    t = datetime(2026, 9, 29)
    df = spark.createDataFrame([("a", "R", "K", t, 1.0), ("b", "R", "K", t, 5.0),
                                ("c", "R", "K2", t, 1.0)], cols)
    assert sorted(r["alert_id"] for r in dedupe_alerts(df, None).collect()) == ["b", "c"]
    time.sleep(0)


# --------------------------------------------------------------------------- Level 4 : métriques
def test_kafka_lag_from_progress():
    from common.metrics import kafka_lag
    src = {"endOffset": {"raw-a": {"0": 10, "1": 5}},
           "latestOffset": '{"raw-a": {"0": 15, "1": 5, "2": 7}}'}
    assert kafka_lag(src) == {"raw-a": 12}
    assert kafka_lag({"endOffset": None, "latestOffset": None}) == {}


def test_country_lag_reported(world):
    lags = world["rep"].lag_by_country
    assert set(lags) >= {"CI", "GH"} and all(0 <= v < 30 for v in lags.values())
    assert world["rep"].dlq_by_dataset["bank_transactions"] == 2


def test_listener_metrics_and_probes(spark, tmp_path):
    pytest.importorskip("prometheus_client", reason="image Spark antérieure au Level 4 : make build-spark")
    import urllib.error
    import urllib.request

    from common.metrics import StreamMetrics
    src = tmp_path / "src"
    _write_json(src, "1.json", [{"value": "{}", "topic": "t", "partition": 0, "offset": 0,
                                 "timestamp": "2026-09-29T10:00:00"}])
    m = StreamMetrics("test_job")
    spark.streams.addListener(m.listener())
    srv = m.serve(0)
    port = srv.server_address[1]

    def get(path):
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}") as r:
                return r.status, r.read().decode()
        except urllib.error.HTTPError as e:
            return e.code, e.read().decode()

    assert get("/ready")[0] == 503 and get("/healthz")[0] == 200   # rien encore démarré
    q = (spark.readStream.schema(KAFKA_SCHEMA).json(str(src)).writeStream
         .queryName("probe_q").format("memory").option("checkpointLocation",
                                                      str(tmp_path / "ck")).start())
    q.processAllAvailable()
    for _ in range(50):          # le listener est asynchrone
        if get("/ready")[0] == 200:
            break
        time.sleep(0.2)
    code, body = get("/ready")
    assert code == 200 and "probe_q" in body
    metrics_text = get("/metrics")[1]
    active = [l for l in metrics_text.splitlines() if l.startswith("waba_stream_query_active{")]
    assert any('stream_job="test_job"' in l and 'query="probe_q"' in l and l.endswith(" 1.0")
               for l in active), active
    assert "waba_stream_input_rows_total" in metrics_text
    q.stop()
    for _ in range(50):
        if get("/ready")[0] == 503:
            break
        time.sleep(0.2)
    assert get("/ready")[0] == 503        # requête arrêtée -> plus prêt
    srv.shutdown()
