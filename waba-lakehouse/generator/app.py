"""Application Streamlit : génération de données WABA Group vers MinIO (raw-landing).

Modes :
  * One-time  : génération immédiate d'un volume donné sur une période donnée ;
  * Continue  : un micro-lot toutes les 10 à 60 secondes (flux « temps réel »).
"""
from __future__ import annotations

import logging
import random
import time
from datetime import datetime, time as dtime, timezone

import pandas as pd
import streamlit as st

from waba_gen import config as C
from waba_gen.scenarios import SCENARIOS
from waba_gen.service import (continuous_tick, generate_dataset, generate_fraud_scenarios,
                              generate_referentials, get_sink, load_store)
from waba_gen.transactions import DATASET_ENTITIES, GenerationError, default_period

logging.basicConfig(level=logging.INFO)

st.set_page_config(page_title="WABA Data Generator", page_icon="🏦", layout="wide")

LABELS = {  # les référentiels en premier : ils doivent précéder les transactions
    "referentials": "Référentiels (customers, accounts, branches, products)",
    "bank_transactions": "Transactions bancaires",
    "insurance_operations": "Opérations d'assurance",
    "mobile_money_payments": "Paiements mobile money",
    "loan_repayments": "Remboursements de crédit",
}


@st.cache_resource
def _sink():
    return get_sink()


def _store():
    # Le store est partagé entre les reruns de la session ; rechargé depuis le volume
    # au premier accès (persisté en parquet par le service).
    if "store" not in st.session_state:
        st.session_state.store = load_store()
    return st.session_state.store


def _log(msg: str) -> None:
    st.session_state.setdefault("history", []).insert(
        0, f"{datetime.now(timezone.utc):%H:%M:%S} UTC — {msg}")
    st.session_state.history = st.session_state.history[:200]


# ---------------------------------------------------------------------------
# Barre latérale : paramètres (énoncé 1.1)
# ---------------------------------------------------------------------------
st.sidebar.title("🏦 WABA Group")
st.sidebar.caption("Générateur de données synthétiques → MinIO `raw-landing`")

dataset = st.sidebar.selectbox("Type de données", list(LABELS), format_func=LABELS.get)
countries = st.sidebar.multiselect("Pays", C.COUNTRIES, default=C.COUNTRIES)
entities = st.sidebar.multiselect("Ligne métier (entité)", C.ENTITY_TYPES,
                                  default=C.ENTITY_TYPES)
if dataset in DATASET_ENTITIES:
    st.sidebar.caption(f"Entités concernées par ce flux : {', '.join(DATASET_ENTITIES[dataset])}")

mode = st.sidebar.radio("Mode de génération", ["One-time", "Continue"], horizontal=True)
anomaly_pct = st.sidebar.slider("Anomalies injectées (%)", 0.0, 10.0, 0.0, 0.5,
                                help="Lignes volontairement invalides ou dupliquées, pour "
                                     "démontrer la validation et l'idempotence Spark.")

store = _store()
st.title("Générateur de données — WABA Group")
cols = st.columns(4)
for col, name in zip(cols, C.REFERENTIALS):
    df = getattr(store, name)
    col.metric(name, f"{len(df):,}".replace(",", " "))
if not store.is_ready and dataset != "referentials":
    st.warning("Aucun référentiel : commencez par générer les **Référentiels**. "
               "Les transactions n'utilisent que des clés existantes.")

# ---------------------------------------------------------------------------
# Formulaire
# ---------------------------------------------------------------------------
if dataset == "referentials":
    if mode == "One-time":
        st.subheader("Référentiels complets")
        c1, c2, c3, c4, c5 = st.columns(5)
        sizes = {
            "customers": c1.number_input("customers", 1_000, 2_000_000,
                                         C.DEFAULT_REFERENTIAL_SIZES["customers"], 10_000),
            "accounts": c2.number_input("accounts", 1_000, 4_000_000,
                                        C.DEFAULT_REFERENTIAL_SIZES["accounts"], 10_000),
            "branches": c3.number_input("branches", 30, 2_000,
                                        C.DEFAULT_REFERENTIAL_SIZES["branches"]),
            "products": c4.number_input("products", 42, 500,
                                        C.DEFAULT_REFERENTIAL_SIZES["products"]),
        }
        seed = c5.number_input("Graine (reproductible)", 0, 10_000, 42)
        if store.is_ready:
            st.info("Un référentiel existe déjà. Avec la même graine et les mêmes tailles, "
                    "la régénération produit exactement les mêmes clés.")
        if st.button("🚀 Générer et envoyer vers MinIO", type="primary"):
            if sizes["accounts"] < sizes["customers"]:
                st.error("Il faut au moins autant de comptes que de clients.")
            else:
                with st.spinner("Génération des référentiels…"):
                    store, res = generate_referentials(_sink(), sizes, int(seed), countries,
                                                       entities)
                st.session_state.store = store
                _log(f"Référentiels : {res.rows:,} lignes → {', '.join(res.files)}")
                st.success(f"{res.rows:,} lignes déposées dans raw-landing.")
                st.rerun()
    n_rows = st.number_input("Nouveaux clients par micro-lot (mode continu)", 1, 50_000, 500) \
        if mode == "Continue" else 0
else:
    default_rows = C.DEFAULT_ROWS[dataset]
    n_rows = st.number_input("Nombre de lignes" + (" par micro-lot" if mode == "Continue" else ""),
                             1, 5_000_000, default_rows if mode == "One-time" else 200,
                             step=1_000 if mode == "One-time" else 50)
    if mode == "One-time":
        d_start, d_end = default_period()
        c1, c2 = st.columns(2)
        start = c1.date_input("Date de début", d_start.date())
        end = c2.date_input("Date de fin (exclue)", d_end.date())
        if st.button("🚀 Générer et envoyer vers MinIO", type="primary",
                     disabled=not store.is_ready):
            try:
                with st.spinner("Génération…"):
                    res = generate_dataset(dataset, store, _sink(), int(n_rows), countries,
                                           entities, datetime.combine(start, dtime.min),
                                           datetime.combine(end, dtime.min),
                                           anomaly_rate=anomaly_pct / 100)
                _log(f"{LABELS[dataset]} : {res.rows:,} lignes, {len(res.files)} fichiers"
                     + (f", anomalies {res.anomalies}" if res.anomalies else ""))
                st.success(f"{res.rows:,} lignes réparties dans {len(res.files)} fichiers.")
                st.dataframe(pd.DataFrame({"fichier": res.files[:50]}), height=240)
            except GenerationError as exc:
                st.error(str(exc))

# ---------------------------------------------------------------------------
# Mode continu : boucle interruptible (le bouton Stop relance le script)
# ---------------------------------------------------------------------------
if mode == "Continue":
    lo, hi = st.slider("Intervalle entre deux micro-lots (secondes)", 10, 60, (10, 60))
    c1, c2 = st.columns(2)
    if c1.button("▶️ Démarrer le flux", type="primary",
                 disabled=not store.is_ready and dataset != "referentials"):
        st.session_state.running = True
    if c2.button("⏹️ Arrêter"):
        st.session_state.running = False
    status, feed = st.empty(), st.empty()
    while st.session_state.get("running"):
        wait = random.randint(lo, hi)
        try:
            res = continuous_tick(dataset, store, _sink(), int(n_rows), countries, entities,
                                  window_seconds=wait, anomaly_rate=anomaly_pct / 100)
            _log(f"[continu] {LABELS[dataset]} : {res.rows} lignes → {', '.join(res.files)}")
        except GenerationError as exc:
            st.session_state.running = False
            st.error(str(exc))
            break
        feed.code("\n".join(st.session_state.history[:15]))
        for remaining in range(wait, 0, -1):
            status.info(f"🟢 Flux actif — prochain micro-lot dans {remaining} s")
            time.sleep(1)

# ---------------------------------------------------------------------------
# Level 3 : scénarios de fraude / conformité (détection temps réel)
# ---------------------------------------------------------------------------
st.divider()
with st.expander("🚨 Level 3 — Scénarios de fraude, AML et liquidité (temps réel)"):
    st.caption("Dépose immédiatement des motifs suspects horodatés « maintenant » : NiFi les "
               "publie dans Kafka et Spark Streaming lève les alertes en moins d'une minute.")
    labels = {"burst": "Rafale > 500 000 XOF sur un même compte (fraude)",
              "unusual_country": "Paiement mobile depuis un pays inhabituel (fraude)",
              "big_claim": "Sinistre > 3 x la prime annuelle (fraude assurance)",
              "aml": "Virement > seuil déclaratif BCEAO (AML)",
              "bank_run": "Retraits massifs : réserve de liquidité entamée (liquidité)"}
    chosen = st.multiselect("Scénarios", list(SCENARIOS), default=list(SCENARIOS),
                            format_func=labels.get)
    sc_countries = st.multiselect("Pays des scénarios", C.COUNTRIES, default=["CI", "SN"])
    if st.button("⚡ Injecter les scénarios", disabled=not store.is_ready or not chosen):
        try:
            for res in generate_fraud_scenarios(store, _sink(), chosen, sc_countries):
                _log(f"[scénario] {LABELS[res.dataset]} : {res.rows} lignes -> "
                     f"{', '.join(res.files)}")
            st.success("Scénarios déposés dans raw-landing : suivre gold-fraud-alerts, "
                       "gold-aml-events et gold-liquidity-alerts dans Kafka UI (:8084).")
        except GenerationError as exc:
            st.error(str(exc))

st.divider()
st.subheader("Journal")
st.code("\n".join(st.session_state.get("history", [])) or "—")
