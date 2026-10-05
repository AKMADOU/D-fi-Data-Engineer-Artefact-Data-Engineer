"""Tests des couches Silver / Gold / réglementaire sur un mini-jeu de données Bronze.

Spark local, sans Iceberg ni MinIO : on teste la logique métier des transformations.
    cd spark && pytest -q tests
"""
from __future__ import annotations

import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "jobs"))

from common.fx import XOF_PER_EUR, fx_rates_df, ghs_per_eur  # noqa: E402
from common.gold_transforms import GOLD_TABLES, build_gold  # noqa: E402
from common.lakehouse import create_table_like_sql, table_schema  # noqa: E402
from common.regulatory import bceao_daily, cima_daily  # noqa: E402
from common.schemas import DATASETS  # noqa: E402
from common.silver_transforms import SILVER_PARTITIONING, build_silver  # noqa: E402

SECRET = "test-secret"
TS = datetime(2026, 5, 10, 12, 0)
ING = datetime(2026, 6, 1, 8, 0)


@pytest.fixture(scope="module")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("medallion-tests")
         .config("spark.sql.session.timeZone", "UTC").config("spark.ui.enabled", "false")
         .config("spark.sql.shuffle.partitions", "4").getOrCreate())
    s.sparkContext.setLogLevel("ERROR")
    yield s


def D(x) -> Decimal:
    return Decimal(str(x)).quantize(Decimal("0.01"))


def frame(spark, name, rows):
    """DataFrame au schéma Bronze exact de `name` ; colonnes absentes = NULL."""
    schema = table_schema(DATASETS[name])
    base = {"_source_file": "s3a://raw-landing/f.csv", "_batch_id": "b", "_ingested_at": ING}
    full = [tuple({**base, **r}.get(f.name) for f in schema.fields) for r in rows]
    return spark.createDataFrame(full, schema)


def cust(cid, cc="CI", ent="BANK", seg="RETAIL"):
    return {"customer_id": cid, "country_code": cc, "entity_type": ent, "segment": seg,
            "kyc_level": "STANDARD", "onboarding_date": date(2020, 1, 1), "region": " Abidjan ",
            "is_active": True}


def acc(aid, cid, cc="CI", typ="CURRENT", ent="BANK", bal=100000, prod="WABA-CI-P-001"):
    return {"account_id": aid, "customer_id": cid, "country_code": cc, "entity_type": ent,
            "account_type": typ, "product_id": prod, "currency": "GHS" if cc == "GH" else "XOF",
            "balance": D(bal), "credit_limit": D(0), "opened_date": date(2021, 1, 1),
            "status": "ACTIVE"}


@pytest.fixture(scope="module")
def bronze(spark):
    customers = [cust("C1"), cust("C2", seg="SME"), cust("C3", "SN"), cust("C4", "GH"),
                 cust("C5", "CI", "INSURANCE"), cust("C6", "CI", "MOBILE_MONEY"),
                 cust("C7", "SN", "MOBILE_MONEY"), cust("C8", "CI", "BANK", "CORPORATE")]
    accounts = [acc("A1", "C1"), acc("A2", "C2"), acc("A3", "C3", "SN"), acc("A4", "C4", "GH"),
                acc("P1", "C5", typ="INSURANCE_POLICY", ent="INSURANCE"),
                acc("W1", "C6", typ="MOBILE_WALLET", ent="MOBILE_MONEY"),
                acc("W2", "C7", "SN", typ="MOBILE_WALLET", ent="MOBILE_MONEY"),
                # Deux prêts CI : 900 000 XOF (en défaut) et 100 000 XOF (sain) -> NPL 90 %
                acc("L1", "C8", typ="LOAN", bal=-900000, prod="WABA-CI-P-002"),
                acc("L2", "C1", typ="LOAN", bal=-100000, prod="WABA-CI-P-002")]
    branches = [{"branch_id": "WABA-CI-B-001", "country_code": "CI", "entity_type": "BANK",
                 "city": "Abidjan", "region": "District d'Abidjan", "branch_type": "FULL_SERVICE",
                 "is_active": True},
                {"branch_id": "WABA-SN-B-001", "country_code": "SN", "entity_type": "BANK",
                 "city": "Dakar", "region": "Dakar", "branch_type": "FULL_SERVICE",
                 "is_active": True}]
    products = [{"product_id": "WABA-CI-P-001", "product_name": "Compte Courant",
                 "product_category": "CURRENT", "entity_type": "BANK", "country_code": "CI",
                 "currency": "XOF", "interest_rate": D(0), "monthly_fee": D(1000),
                 "launch_date": date(2015, 1, 1), "is_active": True},
                {"product_id": "WABA-CI-P-002", "product_name": "Credit Consommation",
                 "product_category": "LOAN", "entity_type": "BANK", "country_code": "CI",
                 "currency": "XOF", "interest_rate": D(12), "monthly_fee": D(0),
                 "launch_date": date(2015, 1, 1), "is_active": True}]

    def txn(tid, aid, amount, status="SUCCESS", cc="CI", ttype="TRANSFER", ben="A2",
            branch="WABA-CI-B-001", ts=TS, cur=None):
        return {"transaction_id": tid, "timestamp": ts, "account_id": aid,
                "beneficiary_account": ben, "branch_id": branch, "country_code": cc,
                "transaction_type": ttype, "amount": D(amount),
                "currency": cur or ("GHS" if cc == "GH" else "XOF"), "channel": None,
                "transaction_status": status, "fee_amount": D(amount * 0.001), "entity_type": "BANK"}

    bank = [txn("T1", "A1", 655957),                               # 1 000 EUR
            txn("T1", "A1", 655957),                               # doublon -> dédupliqué
            txn("T2", "A1", 20_000_000),                           # montant élevé (> 10 M XOF)
            txn("T3", "A1", 655957, ttype="INTERNATIONAL_WIRE", ben="A3"),  # CI -> SN
            txn("T4", "AX", 1000),                                 # compte orphelin
            txn("T5", "A1", 1000, status="FAILED", ben=None),
            txn("T6", "A4", 1300, cc="GH", ben=None, branch="WABA-CI-B-001")]
    ins = [{"operation_id": f"O{i}", "timestamp": TS, "customer_id": "C5", "account_id": "P1",
            "country_code": "CI", "operation_type": op, "product_line": "IARD_AUTO",
            "amount": D(a), "currency": "XOF", "claim_status": cs, "processing_days": pdays,
            "entity_type": "INSURANCE"}
           for i, (op, a, cs, pdays) in enumerate([
               ("PREMIUM_PAYMENT", 100000, None, None), ("POLICY_RENEWAL", 100000, None, None),
               ("CLAIM_PAYMENT", 130000, "PAID", 20), ("CLAIM_SUBMISSION", 50000, "PENDING", 4)])]
    mm = [{"payment_id": pid, "timestamp": TS, "sender_id": s, "receiver_id": r,
           "sender_country": sc, "receiver_country": rc, "amount": D(a), "currency": "XOF",
           "payment_type": pt, "operator": "WABA_PAY", "status": st, "fee_amount": D(a * 0.01),
           "entity_type": "MOBILE_MONEY"}
          for pid, s, r, sc, rc, a, pt, st in [
              ("M1", "C6", "C6", "CI", "CI", 10000, "P2P", "SUCCESS"),
              ("M2", "C6", "C7", "CI", "SN", 65595.7, "CROSS_BORDER_TRANSFER", "SUCCESS"),
              ("M3", "C6", "C6", "CI", "CI", 5000, "AIRTIME", "FAILED"),
              ("M4", "CZ", "C6", "CI", "CI", 5000, "P2P", "SUCCESS")]]   # émetteur orphelin
    loans = [{"repayment_id": rid, "timestamp": ts, "loan_account_id": lid, "customer_id": cid,
              "country_code": "CI", "amount_due": D(50000), "amount_paid": D(paid),
              "currency": "XOF", "due_date": date(2026, 5, 1), "payment_date": pdate,
              "days_overdue": od, "loan_type": "CONSUMER", "repayment_status": st,
              "entity_type": "BANK"}
             for rid, ts, lid, cid, paid, pdate, od, st in [
                 ("R1", datetime(2026, 4, 1), "L1", "C8", 50000, date(2026, 4, 1), 0, "ON_TIME"),
                 ("R2", datetime(2026, 5, 20), "L1", "C8", 0, None, 120, "DEFAULT"),  # dernier
                 ("R3", datetime(2026, 5, 2), "L2", "C1", 50000, date(2026, 5, 2), 1, "LATE")]]
    data = {"customers": customers, "accounts": accounts, "branches": branches,
            "products": products, "bank_transactions": bank, "insurance_operations": ins,
            "mobile_money_payments": mm, "loan_repayments": loans}
    return {n: frame(spark, n, rows) for n, rows in data.items()}


@pytest.fixture(scope="module")
def silver(spark, bronze):
    res = build_silver(bronze, fx_rates_df(spark), SECRET)
    return {k: v.cache() for k, v in res.tables.items()}, res.quarantine.cache()


def rows_by(df, key):
    return {r[key]: r for r in df.collect()}


# ------------------------------------------------------------------ Silver
def test_dedup_and_quarantine(silver):
    tables, quarantine = silver
    bank = rows_by(tables["bank_transactions"], "transaction_id")
    assert "T4" not in bank and len(bank) == 5          # doublon T1 fusionné, T4 écarté
    q = {(r.dataset, r.record_key): r.reason for r in quarantine.collect()}
    assert q[("bank_transactions", "T4")] == "ORPHAN_ACCOUNT"
    assert q[("mobile_money_payments", "M4")] == "ORPHAN_SENDER"


def test_eur_conversion_and_enrichment(silver):
    bank = rows_by(silver[0]["bank_transactions"], "transaction_id")
    assert bank["T1"].amount_eur == D(1000)                               # parité fixe XOF
    assert bank["T6"].amount_eur == D(round(1300 / ghs_per_eur(date(2026, 5, 1)), 2))
    assert bank["T1"].channel == "UNKNOWN"                                # NULL normalisé
    assert bank["T1"].branch_city == "Abidjan" and bank["T1"].customer_segment == "RETAIL"
    assert bank["T2"].is_high_value and not bank["T1"].is_high_value
    assert bank["T3"].is_cross_border and bank["T3"].beneficiary_country == "SN"


def test_pseudonymisation(silver):
    tables = silver[0]
    cols = {c for df in tables.values() for c in df.columns}
    assert not cols & {"customer_id", "account_id", "sender_id", "receiver_id",
                       "beneficiary_account", "loan_account_id"}
    keys = {r.customer_key for r in tables["customers"].collect()}
    assert all(len(k) == 64 for k in keys)                                # SHA-256 hex
    bank = rows_by(tables["bank_transactions"], "transaction_id")
    accounts = rows_by(tables["accounts"], "account_key")
    assert bank["T1"].account_key in accounts                             # jointure préservée


def test_insurance_normalisation(silver):
    ops = rows_by(silver[0]["insurance_operations"], "operation_id")
    assert ops["O0"].claim_status is None and ops["O0"].is_premium
    assert ops["O2"].is_claim_paid and ops["O2"].line_family == "IARD"


def test_loans_npl_flag_and_interest(silver):
    loans = rows_by(silver[0]["loan_repayments"], "repayment_id")
    assert loans["R2"].is_non_performing and not loans["R3"].is_non_performing
    # intérêts perçus = min(payé, encours x 12 % / 12) : 100 000 XOF x 1 % = 1 000 XOF
    assert loans["R3"].interest_received_eur == D(round(1000 / XOF_PER_EUR, 2))


# ------------------------------------------------------------------ Gold
@pytest.fixture(scope="module")
def gold(silver):
    return {k: v.cache() for k, v in build_gold(silver[0], list(GOLD_TABLES)).items()}


def test_all_seven_gold_tables(gold):
    assert set(gold) == set(GOLD_TABLES) and len(gold) == 7
    for name, df in gold.items():
        assert "country_code" in df.columns, name
        assert df.count() > 0, name


def test_npl_ratio(gold):
    ci = rows_by(gold["npl_ratio_by_country"].filter("country_code = 'CI'"), "loan_type")
    # encours en défaut (L1 : 900 000) / encours total (1 000 000) = 90 %
    assert ci["ALL"].npl_ratio == pytest.approx(0.9, abs=1e-3)
    assert ci["ALL"].nb_loans == 2 and ci["ALL"].is_above_threshold


def test_loss_ratio(gold):
    [r] = gold["loss_ratio_by_product"].collect()
    assert r.loss_ratio == pytest.approx(130000 / 200000, abs=1e-3)   # 65 %
    assert not r.is_above_threshold


def test_mobile_money_flow_and_cross_border(gold):
    ci = [r for r in gold["mobile_money_daily_flow"].collect() if r.country_code == "CI"][0]
    assert (ci.nb_transactions, ci.nb_failed, ci.active_users) == (3, 1, 1)
    corridors = {(r.corridor, r.channel): r for r in gold["cross_border_transfers"].collect()}
    assert ("CI-SN", "MOBILE_MONEY") in corridors and ("CI-SN", "BANK_WIRE") in corridors
    assert corridors[("CI-SN", "MOBILE_MONEY")].amount_total_eur == D(100)


def test_daily_volume_has_bank_and_mobile_money(gold):
    ents = {r.entity_type for r in gold["daily_transaction_volume"].collect()}
    assert ents == {"BANK", "MOBILE_MONEY"}


def test_arpu(gold):
    rows = gold["customer_arpu_monthly"].filter("country_code = 'CI'").collect()
    assert rows and all(float(r.arpc_eur) == pytest.approx(
        float(r.revenue_eur) / r.active_customers, abs=0.01) for r in rows)


# ------------------------------------------------------------------ Réglementaire
def test_regulatory_reports(silver):
    b = rows_by(bceao_daily(silver[0], date(2026, 5, 10)), "country_code")
    assert b["CI"].nb_high_value_transactions == 1
    assert not b["CI"].npl_breach        # « as of » : le défaut du 20/05 n'est pas encore connu
    assert b["SN"].nb_bank_transactions == 0                        # pays sans activité : 0
    later = rows_by(bceao_daily(silver[0], date(2026, 5, 25)), "country_code")
    assert later["CI"].npl_breach and later["CI"].nb_bank_transactions == 0
    [c] = cima_daily(silver[0], date(2026, 5, 31)).collect()
    assert c.loss_ratio_mtd == pytest.approx(0.65, abs=1e-3) and c.nb_claims_pending == 1
    assert cima_daily(silver[0], date(2026, 4, 30)).count() == 0    # rien en avril


def test_generated_ddl_parses(spark, silver, gold):
    parser = spark._jsparkSession.sessionState().sqlParser()
    for name, df in silver[0].items():
        parser.parsePlan(create_table_like_sql(df.schema, f"silver.{name}",
                                               SILVER_PARTITIONING[name], "commentaire d'essai"))
    for name, df in gold.items():
        parser.parsePlan(create_table_like_sql(df.schema, f"gold.{name}", ("country_code",),
                                               GOLD_TABLES[name][1]))
