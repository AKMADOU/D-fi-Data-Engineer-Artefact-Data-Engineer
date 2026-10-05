"""Tests de la logique Spark sans MinIO ni Iceberg (Spark local).

    cd spark && pytest -q tests
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

from common.lakehouse import AUDIT_DDL, create_table_sql, merge_sql, table_schema  # noqa: E402
from common.schemas import DATASETS  # noqa: E402
from common.validation import file_stats, prepare_batch, read_landing_csv  # noqa: E402

HEADER = ("transaction_id,timestamp,account_id,beneficiary_account,branch_id,country_code,"
          "transaction_type,amount,currency,channel,transaction_status,fee_amount,entity_type")
GOOD = ("0f8fad5b-d9cb-469f-a165-70867728950e,2026-01-01T10:00:00Z,WABA-CI-A-0000001,"
        "WABA-CI-A-0000002,WABA-CI-B-001,CI,TRANSFER,15000.0,XOF,ATM,SUCCESS,15.0,BANK")


def _row(**over) -> str:
    cols = HEADER.split(",")
    values = dict(zip(cols, GOOD.split(",")))
    values.update(over)
    return ",".join(values[c] for c in cols)


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[1]").appName("tests")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.ui.enabled", "false").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def _batch(spark, tmp_path, lines, name="bank_txn_CI_20260101_01.csv"):
    path = tmp_path / name
    path.write_text("\n".join([HEADER, *lines]) + "\n")
    spec = DATASETS["bank_transactions"]
    raw = read_landing_csv(spark, spec, [str(path)]).cache()
    return raw, prepare_batch(raw, spec, "test-batch")


def _reasons(prepared):
    return {r.record_key: r.reject_reason for r in prepared.rejected.collect()}


def test_valid_row_is_kept_with_types(spark, tmp_path):
    _, b = _batch(spark, tmp_path, [GOOD])
    rows = b.valid.collect()
    assert len(rows) == 1 and b.rejected.count() == 0
    r = rows[0]
    assert str(r.amount) == "15000.00" and r.timestamp.isoformat() == "2026-01-01T10:00:00"
    assert r._batch_id == "test-batch" and r._source_file.endswith(".csv")


@pytest.mark.parametrize("override,reason", [
    ({"currency": "GHS"}, "CURRENCY_COUNTRY_MISMATCH"),
    ({"currency": "EUR"}, "INVALID_CURRENCY"),
    ({"amount": "-10"}, "NEGATIVE_AMOUNT"),
    ({"country_code": "FR"}, "INVALID_COUNTRY"),
    ({"transaction_type": "BRIBE"}, "INVALID_TRANSACTION_TYPE"),
    ({"transaction_status": "DONE"}, "INVALID_TRANSACTION_STATUS"),
    ({"entity_type": "CASINO"}, "INVALID_ENTITY"),
    ({"timestamp": "not-a-date"}, "MALFORMED_ROW"),
    ({"amount": "abc"}, "MALFORMED_ROW"),
    ({"transaction_id": "123"}, "INVALID_KEY_FORMAT"),
])
def test_invalid_rows_are_rejected(spark, tmp_path, override, reason):
    _, b = _batch(spark, tmp_path, [_row(**override)])
    assert b.valid.count() == 0
    [r] = b.rejected.collect()
    assert reason in r.reject_reason.split(";")
    assert r.raw_record  # la ligne d'origine est conservée pour investigation


def test_missing_key_and_short_row(spark, tmp_path):
    _, b = _batch(spark, tmp_path, [_row(transaction_id=""), "only,three,fields"])
    reasons = [r.reject_reason for r in b.rejected.collect()]
    assert any("MISSING_TRANSACTION_ID" in x for x in reasons)
    assert any("MALFORMED_ROW" in x for x in reasons)
    assert b.valid.count() == 0


def test_duplicates_in_batch_are_collapsed(spark, tmp_path):
    raw, b = _batch(spark, tmp_path, [GOOD, GOOD, GOOD])
    assert b.valid.count() == 1
    assert [r.reject_reason for r in b.rejected.collect()] == ["DUPLICATE_IN_BATCH"] * 2
    [stats] = file_stats(raw, b.rejected).collect()
    assert (stats.rows_read, stats.rows_valid, stats.rows_rejected) == (3, 1, 2)


def test_ghana_requires_ghs(spark, tmp_path):
    ok = _row(transaction_id="1b4e28ba-2fa1-41d2-883f-0016d3cca427", country_code="GH",
              currency="GHS", account_id="WABA-GH-A-0000009")
    ko = _row(transaction_id="6fa459ea-ee8a-4ca4-894e-db77e160355e", country_code="GH",
              currency="XOF")
    _, b = _batch(spark, tmp_path, [ok, ko])
    assert [r.transaction_id for r in b.valid.collect()] == [
        "1b4e28ba-2fa1-41d2-883f-0016d3cca427"]
    assert _reasons(b)["6fa459ea-ee8a-4ca4-894e-db77e160355e"] == "CURRENCY_COUNTRY_MISMATCH"


def test_mobile_money_gets_country_code(spark, tmp_path):
    spec = DATASETS["mobile_money_payments"]
    header = ",".join(f.name for f in spec.fields)
    line = ("0f8fad5b-d9cb-469f-a165-70867728950e,2026-01-01T10:00:00Z,WABA-GH-C-000001,"
            "WABA-CI-C-000002,GH,CI,120.50,GHS,CROSS_BORDER_TRANSFER,MTN_PARTNER,SUCCESS,"
            "2.41,MOBILE_MONEY")
    path = tmp_path / "mobile_money_GH_20260101_01.csv"
    path.write_text(f"{header}\n{line}\n")
    b = prepare_batch(read_landing_csv(spark, spec, [str(path)]), spec, "t")
    [r] = b.valid.collect()
    assert r.country_code == "GH"
    assert sorted(f.name for f in table_schema(spec).fields) == sorted(b.valid.columns)


def test_referential_latest_file_wins(spark, tmp_path):
    spec = DATASETS["customers"]
    header = ",".join(f.name for f in spec.fields)
    base = "WABA-CI-C-000001,CI,BANK,{seg},STANDARD,2020-01-01,Abidjan,true"
    (tmp_path / "customers.csv").write_text(f"{header}\n{base.format(seg='RETAIL')}\n")
    (tmp_path / "customers_delta_20260924_01.csv").write_text(
        f"{header}\n{base.format(seg='PREMIUM')}\n")
    paths = [str(tmp_path / "customers.csv"), str(tmp_path / "customers_delta_20260924_01.csv")]
    b = prepare_batch(read_landing_csv(spark, spec, paths), spec, "t")
    [r] = b.valid.collect()
    assert r.segment == "PREMIUM" and r.is_active is True


@pytest.mark.parametrize("name", list(DATASETS))
def test_generated_sql_parses(spark, name):
    """Le DDL et le MERGE générés sont syntaxiquement valides pour Spark 3.5."""
    parser = spark._jsparkSession.sessionState().sqlParser()
    parser.parsePlan(create_table_sql(DATASETS[name]))
    for ddl in AUDIT_DDL:
        parser.parsePlan(ddl)
    parser.parsePlan(merge_sql(DATASETS[name]))
