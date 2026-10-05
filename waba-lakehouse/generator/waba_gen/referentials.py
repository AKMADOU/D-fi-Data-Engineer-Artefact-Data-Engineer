"""Génération des référentiels (customers, accounts, branches, products).

Garanties de cohérence :
  * chaque compte appartient à un client existant, du même pays et de la même entité ;
  * chaque client possède au moins un compte ;
  * une entité n'est générée que dans les pays où elle opère (COUNTRY_ENTITIES) ;
  * chaque (pays, entité) dispose d'au moins une agence ;
  * chaque compte référence un produit existant du même pays / entité / type.

La génération est vectorisée (numpy/pandas) : 500 000 clients et 800 000 comptes
sont produits en quelques secondes. Une graine (`seed`) rend le résultat
reproductible, ce qui permet de régénérer les référentiels sans casser les clés.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd

from . import config as C

log = logging.getLogger(__name__)

_EPOCH = np.datetime64("1970-01-01")


def _random_dates(rng: np.random.Generator, start: date, end: date, n: int) -> np.ndarray:
    """Dates uniformes dans [start, end] (numpy datetime64[D])."""
    s, e = np.datetime64(start, "D"), np.datetime64(end, "D")
    offsets = rng.integers(0, (e - s).astype(int) + 1, size=n)
    return s + offsets.astype("timedelta64[D]")


def _entity_for_countries(rng: np.random.Generator, countries: np.ndarray,
                          allowed_entities: list[str]) -> np.ndarray:
    """Tire une entité par client, restreinte aux entités opérant dans son pays."""
    out = np.empty(len(countries), dtype=object)
    for cc in np.unique(countries):
        mask = countries == cc
        ents = [e for e in C.ENTITY_TYPES
                if e in C.COUNTRY_ENTITIES[cc] and e in allowed_entities]
        if not ents:  # entité filtrée absente de ce pays -> banque par défaut
            ents = ["BANK"]
        probs = np.array([C.ENTITY_PROBS[C.ENTITY_TYPES.index(e)] for e in ents])
        out[mask] = rng.choice(ents, size=mask.sum(), p=probs / probs.sum())
    return out


def _weighted_countries(rng: np.random.Generator, countries: list[str], n: int) -> np.ndarray:
    w = np.array([C.COUNTRY_WEIGHTS[c] for c in countries])
    return rng.choice(countries, size=n, p=w / w.sum())


# ---------------------------------------------------------------------------
# Générateurs unitaires
# ---------------------------------------------------------------------------
def generate_branches(rng: np.random.Generator, n: int = 200,
                      countries: list[str] | None = None) -> pd.DataFrame:
    countries = countries or C.COUNTRIES
    # 1) une agence garantie par couple (pays, entité), 2) le reste réparti au prorata.
    base = [(cc, ent) for cc in countries for ent in C.COUNTRY_ENTITIES[cc]]
    if n < len(base):
        raise ValueError(f"Il faut au moins {len(base)} agences pour couvrir pays x entités")
    extra_cc = _weighted_countries(rng, countries, n - len(base))
    extra_ent = _entity_for_countries(rng, extra_cc, C.ENTITY_TYPES)
    cc_all = np.concatenate([[b[0] for b in base], extra_cc])
    ent_all = np.concatenate([[b[1] for b in base], extra_ent])
    order = np.lexsort((ent_all, cc_all))
    df = pd.DataFrame({"country_code": cc_all[order], "entity_type": ent_all[order]})
    df["seq"] = df.groupby("country_code").cumcount() + 1
    df["branch_id"] = "WABA-" + df["country_code"] + "-B-" + df["seq"].map("{:03d}".format)
    df["city"] = [rng.choice(C.REGIONS[cc]) for cc in df["country_code"]]
    df["region"] = [C.CITY_REGIONS[cc][city] for cc, city in zip(df["country_code"], df["city"])]
    df["branch_type"] = rng.choice(C.BRANCH_TYPES, size=len(df), p=C.BRANCH_TYPE_PROBS)
    # Le Mobile Money n'a pas de guichet physique : agences digitales ou agents.
    mm = df["entity_type"] == "MOBILE_MONEY"
    df.loc[mm, "branch_type"] = rng.choice(["DIGITAL_ONLY", "AGENCY_BANKING"], size=mm.sum())
    df["is_active"] = rng.random(len(df)) > 0.05
    return df[C.COLUMNS["branches"]]


_PRODUCT_NAMES: dict[tuple[str, str], list[str]] = {
    ("BANK", "CURRENT"): ["Compte Courant Particulier", "Compte Courant Premium"],
    ("BANK", "SAVINGS"): ["Livret Epargne Plus", "Plan Epargne Logement"],
    ("BANK", "LOAN"): ["Credit Consommation", "Credit Immobilier", "Credit PME"],
    ("MICROFINANCE", "SAVINGS"): ["Tontine Digitale"],
    ("MICROFINANCE", "LOAN"): ["Microcredit Solidaire", "Credit Agricole Campagne"],
    ("MOBILE_MONEY", "MOBILE_WALLET"): ["WABA Pay Wallet", "WABA Pay Business"],
    ("INSURANCE", "INSURANCE_POLICY"): ["Multirisque Habitation", "Auto Tous Risques",
                                        "Sante Famille", "Assurance Vie Epargne"],
}
_RATE_RANGE: dict[str, tuple[float, float]] = {
    "CURRENT": (0.0, 0.0), "SAVINGS": (2.5, 4.5), "LOAN": (7.0, 18.0),
    "MOBILE_WALLET": (0.0, 0.0), "INSURANCE_POLICY": (0.0, 0.0),
}


def generate_products(rng: np.random.Generator, n: int = 50,
                      countries: list[str] | None = None) -> pd.DataFrame:
    """Catalogue produits par pays : chaque (pays, entité, type de compte) a >= 1 produit."""
    countries = countries or C.COUNTRIES
    rows, extras = [], []
    for cc in countries:
        for ent in C.COUNTRY_ENTITIES[cc]:
            for acc_type in C.ACCOUNT_TYPES_BY_ENTITY[ent][0]:
                names = _PRODUCT_NAMES[(ent, acc_type)]
                rows.append((cc, ent, acc_type, names[0]))
                extras.extend((cc, ent, acc_type, nm) for nm in names[1:])
    if n < len(rows):
        raise ValueError(f"Il faut au moins {len(rows)} produits pour couvrir le catalogue")
    rng.shuffle(extras)
    rows += extras[: n - len(rows)]
    df = pd.DataFrame(rows, columns=["country_code", "entity_type", "product_category",
                                     "product_name"])
    df = df.sort_values(["country_code", "entity_type", "product_category"], kind="stable")
    df["seq"] = df.groupby("country_code").cumcount() + 1
    df["product_id"] = "WABA-" + df["country_code"] + "-P-" + df["seq"].map("{:03d}".format)
    df["currency"] = df["country_code"].map(C.CURRENCY_MAP)
    lo = df["product_category"].map(lambda t: _RATE_RANGE[t][0])
    hi = df["product_category"].map(lambda t: _RATE_RANGE[t][1])
    df["interest_rate"] = (lo + (hi - lo) * rng.random(len(df))).round(2)
    fee_xof = rng.choice([0, 500, 1000, 2500, 5000], size=len(df))
    df["monthly_fee"] = (fee_xof / df["currency"].map(C.XOF_PER_UNIT)).round(2)
    df["launch_date"] = _random_dates(rng, date(2012, 1, 1), date(2025, 6, 30), len(df))
    df["is_active"] = rng.random(len(df)) > 0.1
    return df[C.COLUMNS["products"]].reset_index(drop=True)


def generate_customers(rng: np.random.Generator, n: int, start_index: int = 0,
                       countries: list[str] | None = None,
                       entities: list[str] | None = None,
                       onboarding_range: tuple[date, date] = (date(2010, 1, 1),
                                                              date(2025, 12, 31)),
                       ) -> pd.DataFrame:
    countries = countries or C.COUNTRIES
    entities = entities or C.ENTITY_TYPES
    cc = _weighted_countries(rng, countries, n)
    idx = np.arange(start_index + 1, start_index + n + 1)  # identifiants 1-based
    df = pd.DataFrame({
        # Format de l'énoncé : WABA-CI-C-000001 (6 chiffres, 7+ au-delà d'un million)
        "customer_id": [f"WABA-{c}-C-{i:06d}" for c, i in zip(cc, idx)],
        "country_code": cc,
        "entity_type": _entity_for_countries(rng, cc, entities),
        "segment": rng.choice(C.SEGMENTS, size=n, p=C.SEGMENT_PROBS),
        "kyc_level": rng.choice(C.KYC_LEVELS, size=n, p=C.KYC_PROBS),
        "onboarding_date": _random_dates(rng, *onboarding_range, n),
    })
    df["region"] = [rng.choice(C.REGIONS[c]) for c in df["country_code"]]
    df["is_active"] = rng.random(n) > 0.08
    return df[C.COLUMNS["customers"]]


def generate_accounts(rng: np.random.Generator, customers: pd.DataFrame, n: int,
                      products: pd.DataFrame, start_index: int = 0,
                      max_opened: date = date(2025, 12, 31)) -> pd.DataFrame:
    """Chaque client reçoit un compte, les n - len(customers) restants sont tirés au hasard."""
    if n < len(customers):
        raise ValueError("Le nombre de comptes doit être >= au nombre de clients")
    owners = np.concatenate([np.arange(len(customers)),
                             rng.integers(0, len(customers), size=n - len(customers))])
    rng.shuffle(owners)
    cust = customers.iloc[owners].reset_index(drop=True)
    df = pd.DataFrame({
        "customer_id": cust["customer_id"].to_numpy(),
        "country_code": cust["country_code"].to_numpy(),
        "entity_type": cust["entity_type"].to_numpy(),
    })
    idx = np.arange(start_index + 1, start_index + n + 1)
    df.insert(0, "account_id",
              [f"WABA-{c}-A-{i:07d}" for c, i in zip(df["country_code"], idx)])

    # Type de compte cohérent avec l'entité du client.
    acc_type = np.empty(n, dtype=object)
    for ent, (types, probs) in C.ACCOUNT_TYPES_BY_ENTITY.items():
        m = (df["entity_type"] == ent).to_numpy()
        acc_type[m] = rng.choice(types, size=m.sum(), p=probs)
    df["account_type"] = acc_type

    # Produit : tiré parmi les produits (pays, entité, type) du catalogue.
    grouped = products.groupby(["country_code", "entity_type", "product_category"])["product_id"]
    catalog = {k: v.to_numpy() for k, v in grouped}
    prod = np.empty(n, dtype=object)
    for key, sub in df.groupby(["country_code", "entity_type", "account_type"]).groups.items():
        prod[np.asarray(sub)] = rng.choice(catalog[key], size=len(sub))
    df["product_id"] = prod

    df["currency"] = df["country_code"].map(C.CURRENCY_MAP)
    scale = df["currency"].map(C.XOF_PER_UNIT).to_numpy()
    seg_mult = cust["segment"].map({"RETAIL": 1, "SME": 4, "CORPORATE": 20,
                                    "PREMIUM": 8}).to_numpy()
    balance = rng.lognormal(mean=12.5, sigma=1.4, size=n) * seg_mult
    is_loan = df["account_type"] == "LOAN"
    # Encours de prêt moins dispersé que les dépôts : un ratio NPL pondéré par
    # l'encours reste ainsi stable et réaliste (pas dominé par quelques gros prêts).
    loan_mult = cust["segment"].map({"RETAIL": 1, "SME": 1.5, "CORPORATE": 2.5,
                                     "PREMIUM": 2}).to_numpy()
    principal = rng.lognormal(mean=14.0, sigma=0.4, size=n) * loan_mult
    # Compte prêt : solde = encours restant dû (négatif), plafond = montant accordé.
    df["balance"] = np.where(is_loan, -principal * rng.uniform(0.3, 1.0, n), balance)
    overdraft = np.where(rng.random(n) < 0.3, rng.choice([1e5, 5e5, 1e6, 5e6], n), 0.0)
    df["credit_limit"] = np.select([is_loan, df["account_type"] == "CURRENT"],
                                   [principal, overdraft * seg_mult], 0.0)
    df["balance"] = (df["balance"] / scale).round(2)
    df["credit_limit"] = (df["credit_limit"] / scale).round(2)

    # Ouverture postérieure à l'entrée en relation du client.
    onboard = pd.to_datetime(cust["onboarding_date"]).to_numpy().astype("datetime64[D]")
    span = (np.datetime64(max_opened, "D") - onboard).astype(int)
    df["opened_date"] = onboard + (rng.random(n) * np.maximum(span, 0)).astype(
        "timedelta64[D]")
    status = rng.choice(C.ACCOUNT_STATUSES, size=n, p=C.ACCOUNT_STATUS_PROBS)
    inactive = ~cust["is_active"].to_numpy()
    status[inactive] = rng.choice(["DORMANT", "CLOSED"], size=inactive.sum())
    df["status"] = status
    return df[C.COLUMNS["accounts"]]


# ---------------------------------------------------------------------------
# Magasin de référentiels (état persistant du générateur)
# ---------------------------------------------------------------------------
@dataclass
class ReferentialStore:
    """Référentiels en mémoire + persistance parquet (volume Docker du générateur).

    C'est la source des clés utilisées par les générateurs de transactions :
    une transaction ne peut référencer qu'une clé présente ici, et donc déjà
    déposée dans raw-landing.
    """

    customers: pd.DataFrame = field(default_factory=pd.DataFrame)
    accounts: pd.DataFrame = field(default_factory=pd.DataFrame)
    branches: pd.DataFrame = field(default_factory=pd.DataFrame)
    products: pd.DataFrame = field(default_factory=pd.DataFrame)

    @property
    def is_ready(self) -> bool:
        return all(len(getattr(self, n)) for n in C.REFERENTIALS)

    def frames(self) -> dict[str, pd.DataFrame]:
        return {n: getattr(self, n) for n in C.REFERENTIALS}

    # -- génération ---------------------------------------------------------
    @classmethod
    def generate(cls, sizes: dict[str, int] | None = None, seed: int = 42,
                 countries: list[str] | None = None,
                 entities: list[str] | None = None) -> "ReferentialStore":
        sizes = {**C.DEFAULT_REFERENTIAL_SIZES, **(sizes or {})}
        rng = np.random.default_rng(seed)
        branches = generate_branches(rng, sizes["branches"], countries)
        products = generate_products(rng, sizes["products"], countries)
        customers = generate_customers(rng, sizes["customers"], countries=countries,
                                       entities=entities)
        accounts = generate_accounts(rng, customers, sizes["accounts"], products)
        log.info("referentials generated: %s",
                 {k: len(v) for k, v in [("customers", customers), ("accounts", accounts),
                                         ("branches", branches), ("products", products)]})
        return cls(customers, accounts, branches, products)

    def add_delta(self, n_customers: int, seed: int | None = None,
                  countries: list[str] | None = None,
                  entities: list[str] | None = None,
                  onboarding_day: date | None = None) -> dict[str, pd.DataFrame]:
        """Nouveaux clients (+ ~1,6 compte chacun) : simule l'onboarding quotidien.

        Les identifiants continuent la séquence existante : aucune collision.
        """
        if not self.is_ready:
            raise RuntimeError("Générez d'abord les référentiels complets.")
        rng = np.random.default_rng(seed)
        day = onboarding_day or date.today()
        new_c = generate_customers(rng, n_customers, start_index=len(self.customers),
                                   countries=countries, entities=entities,
                                   onboarding_range=(day, day))
        n_acc = max(n_customers, int(round(n_customers * 1.6)))
        new_a = generate_accounts(rng, new_c, n_acc, self.products,
                                  start_index=len(self.accounts), max_opened=day)
        self.customers = pd.concat([self.customers, new_c], ignore_index=True)
        self.accounts = pd.concat([self.accounts, new_a], ignore_index=True)
        return {"customers": new_c, "accounts": new_a}

    # -- persistance ----------------------------------------------------------
    def save(self, directory: str | Path) -> None:
        directory = Path(directory)
        directory.mkdir(parents=True, exist_ok=True)
        for name, df in self.frames().items():
            df.to_parquet(directory / f"{name}.parquet", index=False)

    @classmethod
    def load(cls, directory: str | Path) -> "ReferentialStore":
        directory = Path(directory)
        frames = {}
        for name in C.REFERENTIALS:
            path = directory / f"{name}.parquet"
            frames[name] = pd.read_parquet(path) if path.exists() else pd.DataFrame()
        return cls(**frames)
