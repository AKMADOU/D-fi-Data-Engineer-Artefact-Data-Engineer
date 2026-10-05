"""Tests du générateur : cohérence référentielle, devises, nomenclature, idempotence des noms."""
from __future__ import annotations

import re
from datetime import datetime

import pandas as pd
import pytest

from waba_gen import config as C
from waba_gen.referentials import ReferentialStore
from waba_gen.storage import LocalSink
from waba_gen.transactions import GenerationError, default_period, generate_events
from waba_gen.writer import write_events, write_referentials

START, END = datetime(2026, 4, 1), datetime(2026, 7, 1)
SMALL = {"customers": 5_000, "accounts": 8_000, "branches": 200, "products": 50}


@pytest.fixture(scope="module")
def store() -> ReferentialStore:
    return ReferentialStore.generate(SMALL, seed=1)


def test_referential_sizes_and_unique_keys(store):
    assert len(store.customers) == 5_000 and len(store.accounts) == 8_000
    assert len(store.branches) == 200 and len(store.products) == 50
    for name, key in [("customers", "customer_id"), ("accounts", "account_id"),
                      ("branches", "branch_id"), ("products", "product_id")]:
        assert getattr(store, name)[key].is_unique, name


def test_id_formats(store):
    assert store.customers["customer_id"].str.match(r"^WABA-[A-Z]{2}-C-\d{6,}$").all()
    assert store.accounts["account_id"].str.match(r"^WABA-[A-Z]{2}-A-\d{7,}$").all()
    assert store.branches["branch_id"].str.match(r"^WABA-[A-Z]{2}-B-\d{3}$").all()


def test_accounts_reference_customers_same_country_and_entity(store):
    merged = store.accounts.merge(store.customers, on="customer_id", suffixes=("", "_c"))
    assert len(merged) == len(store.accounts)  # aucune clé orpheline
    assert (merged["country_code"] == merged["country_code_c"]).all()
    assert (merged["entity_type"] == merged["entity_type_c"]).all()
    assert set(store.customers["customer_id"]) == set(store.accounts["customer_id"])
    assert store.accounts["product_id"].isin(store.products["product_id"]).all()


def test_entities_only_where_they_operate(store):
    for df in (store.customers, store.branches, store.products):
        ok = [e in C.COUNTRY_ENTITIES[c] for c, e in zip(df["country_code"], df["entity_type"])]
        assert all(ok)


def test_currency_matches_country(store):
    assert (store.accounts["currency"] == store.accounts["country_code"].map(C.CURRENCY_MAP)).all()


@pytest.mark.parametrize("dataset", list(C.EVENT_DATASETS))
def test_events_referential_integrity(store, dataset):
    df, anomalies = generate_events(dataset, store, 3_000, C.COUNTRIES, C.ENTITY_TYPES,
                                    START, END, seed=7)
    assert not anomalies and list(df.columns[:-1]) == C.COLUMNS[dataset]
    accounts = set(store.accounts["account_id"])
    customers = set(store.customers["customer_id"])
    checks = {
        "bank_transactions": [("account_id", accounts), ("beneficiary_account", accounts),
                              ("branch_id", set(store.branches["branch_id"]))],
        "insurance_operations": [("account_id", accounts), ("customer_id", customers)],
        "mobile_money_payments": [("sender_id", customers), ("receiver_id", customers)],
        "loan_repayments": [("loan_account_id", accounts), ("customer_id", customers)],
    }[dataset]
    for col, valid in checks:
        assert df[col].isin(valid).all(), col
    country_col = "sender_country" if dataset == "mobile_money_payments" else "country_code"
    assert (df["currency"] == df[country_col].map(C.CURRENCY_MAP)).all()
    ts = pd.to_datetime(df["timestamp"]).dt.tz_localize(None)
    assert ts.min() >= START and ts.max() < END


def test_loans_and_insurance_are_consistent(store):
    loans, _ = generate_events("loan_repayments", store, 2_000, C.COUNTRIES, C.ENTITY_TYPES,
                               START, END, seed=3)
    acc = store.accounts.set_index("account_id")
    assert (acc.loc[loans["loan_account_id"], "account_type"] == "LOAN").all()
    assert (acc.loc[loans["loan_account_id"], "customer_id"].to_numpy()
            == loans["customer_id"].to_numpy()).all()
    default = loans["repayment_status"] == "DEFAULT"
    assert loans.loc[default, "payment_date"].isna().all()
    assert (loans.loc[default, "amount_paid"] == 0).all()
    ins, _ = generate_events("insurance_operations", store, 2_000, C.COUNTRIES,
                             C.ENTITY_TYPES, START, END, seed=3)
    claim = ins["operation_type"].isin(["CLAIM_SUBMISSION", "CLAIM_PAYMENT"])
    assert ins.loc[~claim, "claim_status"].isna().all()
    assert ins.loc[~claim, "processing_days"].isna().all()
    assert not ins.loc[(ins["country_code"] == "GH"), "product_line"].isin(["VIE"]).any()


def test_mobile_money_only_in_operating_countries(store):
    with pytest.raises(GenerationError):
        generate_events("mobile_money_payments", store, 100, ["ML"], C.ENTITY_TYPES,
                        START, END)
    df, _ = generate_events("mobile_money_payments", store, 1_000, C.COUNTRIES,
                            C.ENTITY_TYPES, START, END, seed=2)
    assert set(df["sender_country"]) <= set(C.MOBILE_MONEY_COUNTRIES)


def test_transactions_require_referentials():
    with pytest.raises(GenerationError):
        generate_events("bank_transactions", ReferentialStore(), 10, C.COUNTRIES,
                        C.ENTITY_TYPES, START, END)


def test_file_naming_and_no_overwrite(store, tmp_path):
    sink = LocalSink(tmp_path)
    df, _ = generate_events("bank_transactions", store, 500, ["CI", "SN"], ["BANK"],
                            datetime(2026, 1, 1), datetime(2026, 1, 3), seed=5)
    first = write_events(df, "bank_transactions", sink)
    second = write_events(df, "bank_transactions", sink)
    pattern = re.compile(r"^(CI|SN)/bank_transactions/bank_txn_(CI|SN)_2026010[12]_\d{2}\.csv$")
    assert all(pattern.match(k) for k in first + second)
    assert "CI/bank_transactions/bank_txn_CI_20260101_01.csv" in first
    assert "CI/bank_transactions/bank_txn_CI_20260101_02.csv" in second
    assert not set(first) & set(second)  # la 2e génération n'écrase rien


def test_anomalies_are_injected(store):
    df, anomalies = generate_events("bank_transactions", store, 2_000, C.COUNTRIES,
                                    ["BANK"], START, END, seed=9, anomaly_rate=0.05)
    assert sum(anomalies.values()) == 100
    assert len(df) == 2_000 + anomalies.get("duplicate", 0)


def test_delta_continues_sequences(store, tmp_path):
    local = ReferentialStore(**{k: v.copy() for k, v in store.frames().items()})
    delta = local.add_delta(100, seed=4)
    assert delta["customers"]["customer_id"].is_unique
    assert not delta["customers"]["customer_id"].isin(store.customers["customer_id"]).any()
    assert local.customers["customer_id"].is_unique and local.accounts["account_id"].is_unique
    keys = write_referentials(delta, LocalSink(tmp_path),
                              delta_day=datetime(2026, 9, 24).date())
    assert keys[0] == "referentials/customers/customers_delta_20260924_01.csv"


def test_reproducible_with_seed():
    a = ReferentialStore.generate(SMALL, seed=11)
    b = ReferentialStore.generate(SMALL, seed=11)
    pd.testing.assert_frame_equal(a.accounts, b.accounts)


def test_default_period_is_previous_quarter():
    assert default_period(datetime(2026, 9, 24)) == (datetime(2026, 4, 1), datetime(2026, 7, 1))
    assert default_period(datetime(2026, 2, 1)) == (datetime(2025, 10, 1), datetime(2026, 1, 1))


# ------------------------------------------------------------------ Level 3 : scénarios
def test_fraud_scenarios(store):
    from waba_gen.scenarios import SCENARIOS, generate_scenarios
    now = datetime(2026, 9, 29, 12, 0)
    out = generate_scenarios(store, list(SCENARIOS), ["CI", "GH"], seed=1, now=now)
    bank = out["bank_transactions"]
    accounts = set(store.accounts["account_id"])
    assert bank["account_id"].isin(accounts).all() and bank["beneficiary_account"].isin(accounts).all()
    ts = pd.to_datetime(bank["timestamp"]).dt.tz_localize(None)
    assert ts.max() <= now and ts.min() >= now - pd.Timedelta(minutes=10)
    # burst : >= 3 transferts > 500 000 XOF (équivalent) d'un même compte
    ci = bank[(bank.country_code == "CI") & (bank.transaction_type == "TRANSFER")]
    counts = ci[ci.amount > 500_000].groupby("account_id").size()
    assert (counts >= 3).any()
    # AML : un virement au-dessus du seuil dans chaque devise
    assert (bank[(bank.country_code == "GH") & bank.transaction_type.isin(
        ["TRANSFER", "INTERNATIONAL_WIRE"])].amount > 5000).any()
    # bank run : 30 retraits de comptes distincts
    wd = bank[(bank.transaction_type == "WITHDRAWAL") & (bank.country_code == "CI")]
    assert len(wd) == 30 and wd.account_id.is_unique
    # pays inhabituel : pays d'émission != pays du client, devise conforme au pays d'émission
    mm = out["mobile_money_payments"]
    home = store.customers.set_index("customer_id")["country_code"]
    assert (mm["sender_id"].map(home) != mm["sender_country"]).all()
    assert (mm["currency"] == mm["sender_country"].map(C.CURRENCY_MAP)).all()
    # gros sinistre : sinistre > 3 x primes de la même police
    ins = out["insurance_operations"]
    for _, g in ins.groupby("account_id"):
        prem = g[g.operation_type.isin(["PREMIUM_PAYMENT", "POLICY_RENEWAL"])].amount.sum()
        assert g[g.operation_type == "CLAIM_PAYMENT"].amount.max() > 3 * prem


def test_unusual_country_requires_mobile_money_country(store):
    from waba_gen.scenarios import generate_scenarios
    with pytest.raises(GenerationError):
        generate_scenarios(store, ["unusual_country"], ["ML"])
