"""Scénarios de fraude / conformité (Level 3) : motifs que la détection temps réel doit lever.

Chaque scénario produit quelques lignes **valides** (clés existantes, devise conforme au
pays) horodatées « maintenant », pour être traitées en quasi-temps réel par
NiFi -> Kafka -> Spark Structured Streaming :

| Scénario          | Flux                 | Règle déclenchée (topic)                                   |
|-------------------|----------------------|------------------------------------------------------------|
| burst             | bank_transactions    | >= 2 transactions > 500 000 XOF d'un même compte en 5 min  |
|                   |                      | (gold-fraud-alerts, MULTIPLE_LARGE_TXN)                    |
| unusual_country   | mobile_money         | paiement émis depuis un pays différent du pays du client   |
|                   |                      | (gold-fraud-alerts, UNUSUAL_COUNTRY)                       |
| big_claim         | insurance_operations | sinistre > 3 x primes annuelles de la police               |
|                   |                      | (gold-fraud-alerts, CLAIM_EXCEEDS_PREMIUM)                 |
| aml               | bank_transactions    | virement > 1 000 000 XOF / 5 000 GHS (gold-aml-events)     |
| bank_run          | bank_transactions    | retraits massifs : sorties nettes > 50 % de la réserve de  |
|                   |                      | liquidité du pays en 5 min (gold-liquidity-alerts)         |
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from . import config as C
from .referentials import ReferentialStore
from .transactions import GenerationError, _money, _uuids, finalize_events

SCENARIOS = {
    "burst": "bank_transactions",
    "aml": "bank_transactions",
    "bank_run": "bank_transactions",
    "unusual_country": "mobile_money_payments",
    "big_claim": "insurance_operations",
}
# Paramètres partagés avec les jobs de streaming (spark/jobs/common/streaming.py)
BCEAO_RESERVE_RATIO = 0.03       # réserves obligatoires : 3 % des dépôts
BANK_RUN_RESERVE_SHARE = 0.8     # le scénario consomme 80 % de la réserve (seuil d'alerte : 50 %)


def _active(acc: pd.DataFrame, cc: str, **filters) -> pd.DataFrame:
    m = (acc["country_code"] == cc) & (acc["status"] == "ACTIVE")
    for col, values in filters.items():
        m &= acc[col].isin(values if isinstance(values, (list, tuple)) else [values])
    sub = acc[m]
    if sub.empty:
        raise GenerationError(f"Aucun compte éligible en {cc} pour {filters}")
    return sub.reset_index(drop=True)


def _branch(store: ReferentialStore, cc: str, rng) -> str:
    br = store.branches
    ids = br[(br["country_code"] == cc) & (br["entity_type"] == "BANK")]["branch_id"].to_numpy()
    return str(rng.choice(ids))


def _ts(now: datetime, seconds_before: np.ndarray) -> pd.Series:
    return pd.Series(pd.Timestamp(now) - pd.to_timedelta(seconds_before, unit="s"))


def _bank_rows(store, cc, rng, accounts, amounts_xof, txn_type, now, spread_s=120):
    cur = C.CURRENCY_MAP[cc]
    k = len(amounts_xof)
    benef = _active(store.accounts, cc, account_type=["CURRENT", "SAVINGS"], entity_type="BANK")
    amount = _money(np.asarray(amounts_xof, dtype=float), cur)
    return pd.DataFrame({
        "transaction_id": _uuids(k),
        "timestamp": _ts(now, np.sort(rng.integers(0, spread_s, k))[::-1]),
        "account_id": accounts,
        "beneficiary_account": benef["account_id"].to_numpy()[rng.integers(0, len(benef), k)],
        "branch_id": [_branch(store, cc, rng) for _ in range(k)],
        "country_code": cc, "transaction_type": txn_type, "amount": amount, "currency": cur,
        "channel": "MOBILE_APP", "transaction_status": "SUCCESS",
        "fee_amount": np.round(amount * 0.001, 0 if cur == "XOF" else 2), "entity_type": "BANK",
    })


def burst(store, cc, rng, now):
    acc = _active(store.accounts, cc, account_type=["CURRENT", "SAVINGS"], entity_type="BANK")
    account = acc["account_id"].iat[rng.integers(0, len(acc))]
    amounts = rng.integers(600_000, 900_000, 3)            # 3 x > 500 000 XOF en < 2 min
    return _bank_rows(store, cc, rng, [account] * 3, amounts, "TRANSFER", now)


def aml(store, cc, rng, now):
    acc = _active(store.accounts, cc, account_type=["CURRENT", "SAVINGS"], entity_type="BANK")
    accounts = acc["account_id"].to_numpy()[rng.integers(0, len(acc), 2)]
    amounts = rng.integers(1_500_000, 5_000_000, 2)        # > 1 000 000 XOF (ou 5 000 GHS)
    df = _bank_rows(store, cc, rng, accounts, amounts, "TRANSFER", now, spread_s=600)
    df.loc[1, "transaction_type"] = "INTERNATIONAL_WIRE"
    other = [c for c in C.COUNTRIES if c != cc]
    foreign = _active(store.accounts, str(rng.choice(other)),
                      account_type=["CURRENT", "SAVINGS"], entity_type="BANK")
    df.loc[1, "beneficiary_account"] = foreign["account_id"].iat[0]
    return df


def bank_run(store, cc, rng, now, n=30):
    """Retraits massifs : leur total vaut 80 % de la réserve BCEAO (3 % des dépôts du pays)."""
    dep = _active(store.accounts, cc, account_type=["CURRENT", "SAVINGS"], entity_type="BANK")
    deposits_local = float(dep["balance"].clip(lower=0).sum())
    total_xof = deposits_local * C.XOF_PER_UNIT[C.CURRENCY_MAP[cc]] \
        * BCEAO_RESERVE_RATIO * BANK_RUN_RESERVE_SHARE
    accounts = dep["account_id"].to_numpy()[rng.choice(len(dep), size=min(n, len(dep)),
                                                       replace=False)]
    amounts = np.full(len(accounts), total_xof / len(accounts))
    return _bank_rows(store, cc, rng, accounts, amounts, "WITHDRAWAL", now)


def unusual_country(store, cc, rng, now):
    """Client d'un pays A qui paie depuis un pays B (SIM volée, compte piraté...)."""
    if cc not in C.MOBILE_MONEY_COUNTRIES:
        raise GenerationError(f"Mobile Money non opéré en {cc}")
    wallets = _active(store.accounts, cc, account_type="MOBILE_WALLET")
    xof_mm = [c for c in C.MOBILE_MONEY_COUNTRIES if c != cc and C.CURRENCY_MAP[c] == "XOF"]
    foreign = str(rng.choice(xof_mm))
    receivers = _active(store.accounts, foreign, account_type="MOBILE_WALLET")
    k = 2
    amount = np.round(rng.integers(50_000, 200_000, k).astype(float), 0)
    return pd.DataFrame({
        "payment_id": _uuids(k), "timestamp": _ts(now, rng.integers(0, 60, k)),
        "sender_id": wallets["customer_id"].iat[rng.integers(0, len(wallets))],
        "receiver_id": receivers["customer_id"].to_numpy()[rng.integers(0, len(receivers), k)],
        "sender_country": foreign, "receiver_country": foreign,  # opéré depuis l'étranger
        "amount": amount, "currency": "XOF", "payment_type": "MERCHANT_PAYMENT",
        "operator": "WABA_PAY", "status": "SUCCESS", "fee_amount": 0.0,
        "entity_type": "MOBILE_MONEY",
    })


def big_claim(store, cc, rng, now):
    """Deux primes puis un sinistre de 10 x la prime (> 3 x la prime annuelle cumulée)."""
    pol = _active(store.accounts, cc, account_type="INSURANCE_POLICY")
    row = pol.iloc[rng.integers(0, len(pol))]
    cur = C.CURRENCY_MAP[cc]
    premium = float(_money(np.array([55_000.0]), cur)[0])
    line = "IARD_AUTO"
    ops = ["PREMIUM_PAYMENT", "POLICY_RENEWAL", "CLAIM_PAYMENT"]
    return pd.DataFrame({
        "operation_id": _uuids(3), "timestamp": _ts(now, np.array([100, 60, 5])),
        "customer_id": row["customer_id"], "account_id": row["account_id"],
        "country_code": cc, "operation_type": ops, "product_line": line,
        "amount": [premium, premium, premium * 10], "currency": cur,
        "claim_status": [None, None, "PAID"],
        "processing_days": pd.array([None, None, 3], dtype="Int64"),
        "entity_type": "INSURANCE",
    })


BUILDERS = {"burst": burst, "aml": aml, "bank_run": bank_run,
            "unusual_country": unusual_country, "big_claim": big_claim}


def generate_scenarios(store: ReferentialStore, names: list[str], countries: list[str],
                       seed: int | None = None,
                       now: datetime | None = None) -> dict[str, pd.DataFrame]:
    """Retourne {dataset: DataFrame prêt à écrire} pour les scénarios demandés."""
    if not store.is_ready:
        raise GenerationError("Référentiels absents : générez-les d'abord.")
    unknown = sorted(set(names) - set(SCENARIOS))
    if unknown:
        raise GenerationError(f"Scénarios inconnus : {unknown} (possibles : {list(SCENARIOS)})")
    rng = np.random.default_rng(seed)
    now = now or datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=1)
    frames: dict[str, list[pd.DataFrame]] = {}
    for name in names:
        eligible = [c for c in countries
                    if name != "unusual_country" or c in C.MOBILE_MONEY_COUNTRIES]
        if not eligible:
            raise GenerationError(f"{name} : aucun pays éligible parmi {countries}")
        for cc in eligible:
            frames.setdefault(SCENARIOS[name], []).append(BUILDERS[name](store, cc, rng, now))
    return {ds: finalize_events(pd.concat(dfs, ignore_index=True)) for ds, dfs in frames.items()}
