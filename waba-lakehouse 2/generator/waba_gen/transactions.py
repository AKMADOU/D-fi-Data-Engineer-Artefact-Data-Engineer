"""Génération des flux transactionnels (A.4 à A.7) à partir du ReferentialStore.

Toutes les clés (account_id, customer_id, branch_id) sont tirées dans les
référentiels : aucune clé orpheline n'est possible, hors anomalies injectées
volontairement (qui sont alors invalides et rejetées par la validation Spark).
"""
from __future__ import annotations

import logging
import uuid
import zlib
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from . import config as C
from .referentials import ReferentialStore

log = logging.getLogger(__name__)

TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"  # ISO 8601, UTC

# Entités possibles pour chaque flux (le filtre « ligne métier » de l'UI s'y applique).
DATASET_ENTITIES: dict[str, list[str]] = {
    "bank_transactions": ["BANK"],
    "insurance_operations": ["INSURANCE"],
    "mobile_money_payments": ["MOBILE_MONEY"],
    "loan_repayments": ["BANK", "MICROFINANCE"],
}


class GenerationError(ValueError):
    """Paramètres incompatibles (ex. Mobile Money demandé pour le Mali)."""


# ---------------------------------------------------------------------------
# Utilitaires
# ---------------------------------------------------------------------------
def _uuids(n: int) -> list[str]:
    return [str(uuid.uuid4()) for _ in range(n)]


def _timestamps(rng: np.random.Generator, start: datetime, end: datetime,
                n: int) -> pd.Series:
    span = max(int((end - start).total_seconds()), 1)
    secs = rng.integers(0, span, size=n)
    return pd.Series(pd.Timestamp(start) + pd.to_timedelta(secs, unit="s"))


def _stratified_choice(rng: np.random.Generator, values: list[str], probs: list[float],
                       k: int) -> np.ndarray:
    """Tirage « à effectifs exacts » : round(p x k) occurrences de chaque valeur, mélangées.

    Contrairement à un tirage indépendant, la proportion obtenue est exactement la
    cible, même sur un petit échantillon : les KPIs réglementaires (NPL, loss ratio)
    restent dans des fourchettes réalistes pour chaque pays."""
    raw = np.asarray(probs, dtype=float) * k
    counts = np.floor(raw).astype(int)
    remainder = k - counts.sum()
    counts[np.argsort(-(raw - counts))[:remainder]] += 1
    out = np.repeat(np.asarray(values, dtype=object), counts)
    rng.shuffle(out)
    return out


def _split_by_country(rng: np.random.Generator, n: int, countries: list[str]) -> dict[str, int]:
    w = np.array([C.COUNTRY_WEIGHTS[c] for c in countries])
    counts = rng.multinomial(n, w / w.sum())
    return {c: int(k) for c, k in zip(countries, counts) if k > 0}


def _money(values: np.ndarray, currency: str) -> np.ndarray:
    """Convertit un montant exprimé en XOF vers la devise, arrondi à l'unité monétaire."""
    scaled = values / C.XOF_PER_UNIT[currency]
    return np.round(scaled, 0 if currency == "XOF" else 2)


def _eligible_countries(dataset: str, countries: list[str], entities: list[str],
                        pools: dict[str, pd.DataFrame]) -> list[str]:
    ents = [e for e in DATASET_ENTITIES[dataset] if e in entities]
    if not ents:
        raise GenerationError(
            f"{dataset} concerne les entités {DATASET_ENTITIES[dataset]} : "
            f"aucune n'est sélectionnée ({entities}).")
    eligible = [c for c in countries if c in pools and len(pools[c])]
    if not eligible:
        raise GenerationError(
            f"Aucun pays sélectionné n'a de comptes éligibles pour {dataset} "
            f"(ex. Mobile Money uniquement en {C.MOBILE_MONEY_COUNTRIES}).")
    return eligible


def _pools(accounts: pd.DataFrame, mask: pd.Series) -> dict[str, pd.DataFrame]:
    sub = accounts[mask & (accounts["status"] == "ACTIVE")]
    return {cc: g.reset_index(drop=True) for cc, g in sub.groupby("country_code")}


# ---------------------------------------------------------------------------
# Générateurs par flux
# ---------------------------------------------------------------------------
def gen_bank_transactions(store: ReferentialStore, n: int, countries: list[str],
                          entities: list[str], start: datetime, end: datetime,
                          rng: np.random.Generator) -> pd.DataFrame:
    acc = store.accounts
    pools = _pools(acc, (acc["entity_type"] == "BANK")
                   & acc["account_type"].isin(["CURRENT", "SAVINGS"]))
    eligible = _eligible_countries("bank_transactions", countries, entities, pools)
    br = store.branches
    frames = []
    for cc, k in _split_by_country(rng, n, eligible).items():
        pool, cur = pools[cc], C.CURRENCY_MAP[cc]
        branches = br[(br["country_code"] == cc) & (br["entity_type"] == "BANK")
                      & br["is_active"]]["branch_id"].to_numpy()
        if len(branches) == 0:
            branches = br[br["country_code"] == cc]["branch_id"].to_numpy()
        txn_type = rng.choice(C.BANK_TXN_TYPES, size=k, p=C.BANK_TXN_PROBS)
        # Bénéficiaire : même pays, sauf virement international -> autre pays.
        benef = pool["account_id"].to_numpy()[rng.integers(0, len(pool), k)]
        intl = txn_type == "INTERNATIONAL_WIRE"
        foreign = [c for c in pools if c != cc]
        if intl.any() and foreign:
            fcc = rng.choice(foreign, size=intl.sum())
            benef[intl] = [pools[c]["account_id"].iat[rng.integers(0, len(pools[c]))]
                           for c in fcc]
        amount = _money(np.maximum(500, rng.lognormal(12, 1.5, k)), cur)
        status = rng.choice(C.TXN_STATUSES, size=k, p=C.TXN_PROBS)
        frames.append(pd.DataFrame({
            "transaction_id": _uuids(k),
            "timestamp": _timestamps(rng, start, end, k),
            "account_id": pool["account_id"].to_numpy()[rng.integers(0, len(pool), k)],
            "beneficiary_account": benef,
            "branch_id": rng.choice(branches, size=k),
            "country_code": cc,
            "transaction_type": txn_type,
            "amount": amount,
            "currency": cur,
            "channel": rng.choice(C.CHANNELS, size=k, p=C.CHANNEL_PROBS),
            "transaction_status": status,
            "fee_amount": np.where(status == "SUCCESS",
                                   np.round(amount * 0.001, 0 if cur == "XOF" else 2), 0.0),
            "entity_type": "BANK",
        }))
    return pd.concat(frames, ignore_index=True)


def gen_insurance_operations(store: ReferentialStore, n: int, countries: list[str],
                             entities: list[str], start: datetime, end: datetime,
                             rng: np.random.Generator) -> pd.DataFrame:
    acc = store.accounts
    pools = _pools(acc, acc["account_type"] == "INSURANCE_POLICY")
    eligible = _eligible_countries("insurance_operations", countries, entities, pools)
    frames = []
    for cc, k in _split_by_country(rng, n, eligible).items():
        pool, cur = pools[cc], C.CURRENCY_MAP[cc]
        pick = rng.integers(0, len(pool), k)
        lines = C.PRODUCT_LINES_GH if cc == "GH" else C.PRODUCT_LINES_UEMOA
        product_line = _stratified_choice(rng, lines, [1 / len(lines)] * len(lines), k)
        # Mix d'opérations exact au sein de chaque produit : loss ratio stable par produit.
        op = np.empty(k, dtype=object)
        for line in lines:
            m = product_line == line
            op[m] = _stratified_choice(rng, C.INSURANCE_OP_TYPES, C.INSURANCE_OP_PROBS, m.sum())
        # Montants calibrés pour que sinistres payés / primes ≈ loss ratio cible du pays :
        # E[sinistre] = cible x E[prime] x (part des primes / part des paiements de sinistre).
        sigma = 0.35
        premium_mean = 55_000.0
        claim_mean = (C.LOSS_RATIO_TARGET[cc] * premium_mean
                      * C.INSURANCE_PREMIUM_SHARE / C.INSURANCE_CLAIM_PAYMENT_SHARE)
        mu = np.select([op == "CLAIM_SUBMISSION", op == "CLAIM_PAYMENT",
                        op == "POLICY_CANCELLATION"],
                       [np.log(claim_mean), np.log(claim_mean), np.log(premium_mean * 0.3)],
                       np.log(premium_mean)) - sigma ** 2 / 2
        amount = _money(np.maximum(1000, rng.lognormal(mu, sigma)), cur)
        claim_status = np.full(k, None, dtype=object)
        sub = op == "CLAIM_SUBMISSION"
        claim_status[sub] = rng.choice(C.CLAIM_SUBMISSION_STATUSES, size=sub.sum(),
                                       p=C.CLAIM_SUBMISSION_PROBS)
        claim_status[op == "CLAIM_PAYMENT"] = "PAID"
        is_claim = sub | (op == "CLAIM_PAYMENT")
        days = np.where(op == "CLAIM_PAYMENT", rng.integers(5, 61, k), rng.integers(0, 31, k))
        frames.append(pd.DataFrame({
            "operation_id": _uuids(k),
            "timestamp": _timestamps(rng, start, end, k),
            "customer_id": pool["customer_id"].to_numpy()[pick],
            "account_id": pool["account_id"].to_numpy()[pick],
            "country_code": cc,
            "operation_type": op,
            "product_line": product_line,
            "amount": amount,
            "currency": cur,
            "claim_status": claim_status,
            # NULL si l'opération n'est pas un sinistre (schéma A.5)
            "processing_days": pd.Series(days, dtype="Int64").where(is_claim, pd.NA),
            "entity_type": "INSURANCE",
        }))
    return pd.concat(frames, ignore_index=True)


def gen_mobile_money_payments(store: ReferentialStore, n: int, countries: list[str],
                              entities: list[str], start: datetime, end: datetime,
                              rng: np.random.Generator) -> pd.DataFrame:
    acc = store.accounts
    pools = _pools(acc, acc["account_type"] == "MOBILE_WALLET")
    eligible = _eligible_countries("mobile_money_payments", countries, entities, pools)
    frames = []
    for cc, k in _split_by_country(rng, n, eligible).items():
        pool, cur = pools[cc], C.CURRENCY_MAP[cc]
        ptype = rng.choice(C.MM_PAYMENT_TYPES, size=k, p=C.MM_PAYMENT_PROBS)
        receiver_cc = np.full(k, cc, dtype=object)
        cross = ptype == "CROSS_BORDER_TRANSFER"
        foreign = [c for c in pools if c != cc]
        if foreign:
            receiver_cc[cross] = rng.choice(foreign, size=cross.sum())
        else:  # un seul pays MM disponible : pas de transfrontalier possible
            ptype[cross] = "P2P"
        receivers = [pools[c]["customer_id"].iat[rng.integers(0, len(pools[c]))]
                     for c in receiver_cc]
        amount = _money(np.maximum(100, rng.lognormal(9.5, 1.3, k)), cur)
        status = rng.choice(C.MM_STATUSES, size=k, p=C.MM_STATUS_PROBS)
        rate = np.array([C.MM_FEE_RATES[t] for t in ptype])
        frames.append(pd.DataFrame({
            "payment_id": _uuids(k),
            "timestamp": _timestamps(rng, start, end, k),
            "sender_id": pool["customer_id"].to_numpy()[rng.integers(0, len(pool), k)],
            "receiver_id": receivers,
            "sender_country": cc,
            "receiver_country": receiver_cc,
            "amount": amount,
            "currency": cur,
            "payment_type": ptype,
            "operator": rng.choice(C.MM_OPERATORS, size=k, p=C.MM_OPERATOR_PROBS),
            "status": status,
            "fee_amount": np.where(status == "SUCCESS",
                                   np.round(amount * rate, 0 if cur == "XOF" else 2), 0.0),
            "entity_type": "MOBILE_MONEY",
        }))
    return pd.concat(frames, ignore_index=True)


def gen_loan_repayments(store: ReferentialStore, n: int, countries: list[str],
                        entities: list[str], start: datetime, end: datetime,
                        rng: np.random.Generator) -> pd.DataFrame:
    acc = store.accounts
    ents = [e for e in DATASET_ENTITIES["loan_repayments"] if e in entities]
    pools = _pools(acc, (acc["account_type"] == "LOAN") & acc["entity_type"].isin(ents))
    eligible = _eligible_countries("loan_repayments", countries, entities, pools)
    frames = []
    for cc, k in _split_by_country(rng, n, eligible).items():
        pool, cur = pools[cc], C.CURRENCY_MAP[cc]
        # Sans remise : une échéance par prêt et par lot, donc la part de prêts en
        # défaut du lot est exactement le taux cible du pays.
        pick = rng.choice(len(pool), size=k, replace=k > len(pool))
        entity = pool["entity_type"].to_numpy()[pick]
        loan_ids = pool["account_id"].to_numpy()[pick]
        # Type de prêt déterministe par compte : tous les remboursements d'un même
        # prêt portent le même loan_type (cohérence nécessaire au calcul du NPL).
        loan_type = np.array([_loan_type(a, e) for a, e in zip(loan_ids, entity)])
        mult = np.select([loan_type == "MORTGAGE", loan_type == "SME"], [4.0, 2.5], 1.0)
        due = _money(np.maximum(2000, rng.lognormal(11, 0.9, k) * mult), cur)
        p_default = C.DEFAULT_RATE[cc]
        status = _stratified_choice(rng, C.REPAYMENT_STATUSES,
                                    [1 - C.LATE_RATE - p_default, C.LATE_RATE, p_default], k)
        ts = _timestamps(rng, start, end, k)
        event_day = ts.dt.normalize()
        # L'horodatage est la date d'observation ; l'échéance est déduite à rebours
        # pour que timestamp reste dans la période simulée.
        overdue = np.select([status == "LATE", status == "DEFAULT"],
                            [rng.integers(1, 91, k), rng.integers(91, 181, k)], 0)
        early = rng.integers(0, 6, k)
        due_date = np.where(status == "ON_TIME",
                            event_day + pd.to_timedelta(early, unit="D"),
                            event_day - pd.to_timedelta(overdue, unit="D"))
        paid = status != "DEFAULT"
        frames.append(pd.DataFrame({
            "repayment_id": _uuids(k),
            "timestamp": ts,
            "loan_account_id": loan_ids,
            "customer_id": pool["customer_id"].to_numpy()[pick],
            "country_code": cc,
            "amount_due": due,
            "amount_paid": np.where(paid, due, 0.0),
            "currency": cur,
            "due_date": pd.to_datetime(due_date).date,
            "payment_date": pd.Series(event_day.dt.date).where(paid, None),
            "days_overdue": overdue,
            "loan_type": loan_type,
            "repayment_status": status,
            "entity_type": entity,
        }))
    return pd.concat(frames, ignore_index=True)


def _loan_type(account_id: str, entity: str) -> str:
    """Type de prêt stable pour un compte (hash CRC32 -> tirage pondéré)."""
    types = C.LOAN_TYPES_BY_ENTITY[entity]
    u = (zlib.crc32(account_id.encode()) % 10_000) / 10_000
    cumulative = np.cumsum(C.LOAN_TYPE_WEIGHTS[entity])
    return types[int(np.searchsorted(cumulative, u, side="right"))]


GENERATORS = {
    "bank_transactions": gen_bank_transactions,
    "insurance_operations": gen_insurance_operations,
    "mobile_money_payments": gen_mobile_money_payments,
    "loan_repayments": gen_loan_repayments,
}

ID_COLUMN = {"bank_transactions": "transaction_id", "insurance_operations": "operation_id",
             "mobile_money_payments": "payment_id", "loan_repayments": "repayment_id"}
AMOUNT_COLUMN = {"bank_transactions": "amount", "insurance_operations": "amount",
                 "mobile_money_payments": "amount", "loan_repayments": "amount_due"}
COUNTRY_COLUMN = {"mobile_money_payments": "sender_country"}


# ---------------------------------------------------------------------------
# Anomalies (démontrer la validation / l'idempotence côté Spark)
# ---------------------------------------------------------------------------
def inject_anomalies(df: pd.DataFrame, dataset: str, rate: float,
                     rng: np.random.Generator) -> tuple[pd.DataFrame, dict[str, int]]:
    """Corrompt ~`rate` des lignes. Chaque anomalie doit être rejetée ou dédupliquée."""
    if rate <= 0 or df.empty:
        return df, {}
    df = df.copy()
    for col in ("timestamp", "currency", ID_COLUMN[dataset], AMOUNT_COLUMN[dataset]):
        df[col] = df[col].astype(object)
    n_bad = max(1, int(len(df) * rate))
    idx = rng.choice(len(df), size=min(n_bad, len(df)), replace=False)
    kinds = rng.choice(["bad_currency", "negative_amount", "missing_id",
                        "bad_timestamp", "duplicate"], size=len(idx))
    counts: dict[str, int] = {}
    dup_rows = []
    for i, kind in zip(idx, kinds):
        kind = str(kind)
        counts[kind] = counts.get(kind, 0) + 1
        if kind == "bad_currency":
            df.at[i, "currency"] = "GHS" if df.at[i, "currency"] == "XOF" else "XOF"
        elif kind == "negative_amount":
            df.at[i, AMOUNT_COLUMN[dataset]] = -abs(float(df.at[i, AMOUNT_COLUMN[dataset]]))
        elif kind == "missing_id":
            df.at[i, ID_COLUMN[dataset]] = None
        elif kind == "bad_timestamp":
            df.at[i, "timestamp"] = "2026-13-45T99:61:00Z"
        else:
            dup_rows.append(df.iloc[[i]])
    if dup_rows:
        df = pd.concat([df, *dup_rows], ignore_index=True)
    return df, counts


# ---------------------------------------------------------------------------
# Point d'entrée
# ---------------------------------------------------------------------------
def finalize_events(df: pd.DataFrame) -> pd.DataFrame:
    """Tri chronologique, colonne technique de nommage des fichiers, horodatage ISO 8601."""
    df = df.sort_values("timestamp", kind="stable").reset_index(drop=True)
    df["_file_date"] = df["timestamp"].dt.strftime("%Y%m%d")
    df["timestamp"] = df["timestamp"].dt.strftime(TS_FORMAT)
    return df


def generate_events(dataset: str, store: ReferentialStore, n_rows: int,
                    countries: list[str], entities: list[str],
                    start: datetime, end: datetime, seed: int | None = None,
                    anomaly_rate: float = 0.0) -> tuple[pd.DataFrame, dict[str, int]]:
    """Génère `n_rows` événements, retourne (DataFrame prêt à écrire, anomalies)."""
    if dataset not in GENERATORS:
        raise GenerationError(f"Dataset inconnu : {dataset}")
    if not store.is_ready:
        raise GenerationError("Référentiels absents : générez-les avant les transactions.")
    if end <= start:
        raise GenerationError("La date de fin doit être postérieure à la date de début.")
    rng = np.random.default_rng(seed)
    df = finalize_events(GENERATORS[dataset](store, n_rows, countries, entities, start, end, rng))
    df, anomalies = inject_anomalies(df, dataset, anomaly_rate, rng)
    log.info("generated %s rows for %s (anomalies=%s)", len(df), dataset, anomalies)
    return df, anomalies


def default_period(today: datetime | None = None) -> tuple[datetime, datetime]:
    """« Dernier trimestre » : le trimestre civil complet précédant `today`."""
    today = today or datetime.utcnow()
    q_start_month = 3 * ((today.month - 1) // 3) + 1
    current_q = datetime(today.year, q_start_month, 1)
    prev_month = current_q - timedelta(days=1)
    prev_q_month = 3 * ((prev_month.month - 1) // 3) + 1
    return datetime(prev_month.year, prev_q_month, 1), current_q
