"""Configuration métier du WABA Group (source unique de vérité pour le générateur).

Les valeurs reprennent le script de référence de l'annexe A.8 et le tableau
« Contexte & Mission » (quelles entités opèrent dans quels pays).
"""
from __future__ import annotations

# --- Pays & devises ---------------------------------------------------------
COUNTRIES: list[str] = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH"]
UEMOA: list[str] = ["CI", "SN", "ML", "BF", "GN", "TG", "BJ"]
CURRENCY_MAP: dict[str, str] = {c: "XOF" for c in UEMOA} | {"GH": "GHS"}
VALID_CURRENCIES: set[str] = {"XOF", "GHS"}

# Poids d'activité par pays (utilisés pour répartir le nombre de lignes).
COUNTRY_WEIGHTS: dict[str, float] = {
    "CI": 0.25, "SN": 0.15, "GH": 0.15, "ML": 0.10,
    "BF": 0.10, "BJ": 0.09, "TG": 0.08, "GN": 0.08,
}

# Facteur de conversion approximatif XOF -> devise locale, pour générer des
# montants réalistes en GHS (1 GHS ~ 50 XOF). Valeur fictive, non réglementaire.
XOF_PER_UNIT: dict[str, float] = {"XOF": 1.0, "GHS": 50.0}

# --- Entités ------------------------------------------------------------------
ENTITY_TYPES: list[str] = ["BANK", "INSURANCE", "MOBILE_MONEY", "MICROFINANCE"]
ENTITY_PROBS: list[float] = [0.55, 0.20, 0.18, 0.07]

# Présence des entités par pays (tableau « Contexte & Mission »):
#   Mobile Money : CI, SN, BF, GH  |  Microfinance : ML, GN, BF
#   Banque & Assurance : les 8 pays (Assurance Vie = UEMOA, IARD = UEMOA + GH)
MOBILE_MONEY_COUNTRIES: list[str] = ["CI", "SN", "BF", "GH"]
MICROFINANCE_COUNTRIES: list[str] = ["ML", "GN", "BF"]
COUNTRY_ENTITIES: dict[str, list[str]] = {
    cc: ["BANK", "INSURANCE"]
    + (["MOBILE_MONEY"] if cc in MOBILE_MONEY_COUNTRIES else [])
    + (["MICROFINANCE"] if cc in MICROFINANCE_COUNTRIES else [])
    for cc in COUNTRIES
}

# Types de comptes ouverts par chaque entité (avec probabilités).
ACCOUNT_TYPES_BY_ENTITY: dict[str, tuple[list[str], list[float]]] = {
    "BANK": (["CURRENT", "SAVINGS", "LOAN"], [0.45, 0.30, 0.25]),
    "MICROFINANCE": (["SAVINGS", "LOAN"], [0.50, 0.50]),
    "MOBILE_MONEY": (["MOBILE_WALLET"], [1.0]),
    "INSURANCE": (["INSURANCE_POLICY"], [1.0]),
}

SEGMENTS: list[str] = ["RETAIL", "SME", "CORPORATE", "PREMIUM"]
SEGMENT_PROBS: list[float] = [0.65, 0.20, 0.10, 0.05]
KYC_LEVELS: list[str] = ["BASIC", "STANDARD", "ENHANCED"]
KYC_PROBS: list[float] = [0.3, 0.5, 0.2]
ACCOUNT_STATUSES: list[str] = ["ACTIVE", "DORMANT", "FROZEN", "CLOSED"]
ACCOUNT_STATUS_PROBS: list[float] = [0.85, 0.07, 0.03, 0.05]
BRANCH_TYPES: list[str] = ["FULL_SERVICE", "DIGITAL_ONLY", "AGENCY_BANKING", "ATM_POINT"]
BRANCH_TYPE_PROBS: list[float] = [0.45, 0.15, 0.25, 0.15]

# --- Géographie (ville -> région administrative) -----------------------------
CITY_REGIONS: dict[str, dict[str, str]] = {
    "CI": {"Abidjan": "District d'Abidjan", "Bouake": "Gbeke",
           "Yamoussoukro": "District de Yamoussoukro", "San Pedro": "San-Pedro",
           "Korhogo": "Poro"},
    "SN": {"Dakar": "Dakar", "Thies": "Thies", "Ziguinchor": "Ziguinchor",
           "Saint-Louis": "Saint-Louis", "Kaolack": "Kaolack"},
    "ML": {"Bamako": "District de Bamako", "Sikasso": "Sikasso", "Segou": "Segou",
           "Mopti": "Mopti", "Tombouctou": "Tombouctou"},
    "BF": {"Ouagadougou": "Centre", "Bobo-Dioulasso": "Hauts-Bassins",
           "Koudougou": "Centre-Ouest", "Banfora": "Cascades"},
    "GN": {"Conakry": "Conakry", "Nzerekore": "Nzerekore", "Kindia": "Kindia",
           "Kankan": "Kankan"},
    "TG": {"Lome": "Maritime", "Sokode": "Centrale", "Kara": "Kara",
           "Atakpame": "Plateaux"},
    "BJ": {"Cotonou": "Littoral", "Porto-Novo": "Oueme", "Parakou": "Borgou",
           "Abomey-Calavi": "Atlantique"},
    "GH": {"Accra": "Greater Accra", "Kumasi": "Ashanti", "Tamale": "Northern",
           "Cape Coast": "Central", "Sunyani": "Bono"},
}
REGIONS: dict[str, list[str]] = {cc: list(v) for cc, v in CITY_REGIONS.items()}

# --- Transactions bancaires (A.4) --------------------------------------------
TXN_STATUSES: list[str] = ["SUCCESS", "FAILED", "REVERSED"]
TXN_PROBS: list[float] = [0.92, 0.05, 0.03]
BANK_TXN_TYPES: list[str] = ["TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT",
                             "INTERNATIONAL_WIRE"]
BANK_TXN_PROBS: list[float] = [0.35, 0.30, 0.15, 0.15, 0.05]
CHANNELS: list[str] = ["BRANCH", "ATM", "MOBILE_APP", "INTERNET_BANKING", "USSD"]
CHANNEL_PROBS: list[float] = [0.20, 0.15, 0.35, 0.20, 0.10]

# --- Assurance (A.5) ----------------------------------------------------------
INSURANCE_OP_TYPES: list[str] = ["PREMIUM_PAYMENT", "CLAIM_SUBMISSION", "CLAIM_PAYMENT",
                                 "POLICY_RENEWAL", "POLICY_CANCELLATION"]
INSURANCE_OP_PROBS: list[float] = [0.45, 0.18, 0.12, 0.18, 0.07]
# Primes acquises = PREMIUM_PAYMENT + POLICY_RENEWAL (probabilité cumulée)
INSURANCE_PREMIUM_SHARE: float = 0.45 + 0.18
INSURANCE_CLAIM_PAYMENT_SHARE: float = 0.12
# Loss ratio cible par pays (sinistres payés / primes acquises), calibré dans la
# fourchette réaliste demandée par l'énoncé (50 % - 85 %). Seuil de vigilance CIMA : 70 %.
LOSS_RATIO_TARGET: dict[str, float] = {
    "CI": 0.62, "SN": 0.58, "ML": 0.74, "BF": 0.69,
    "GN": 0.78, "TG": 0.66, "BJ": 0.60, "GH": 0.67,
}
PRODUCT_LINES_UEMOA: list[str] = ["VIE", "IARD_AUTO", "IARD_HABITATION", "IARD_SANTE",
                                  "PREVOYANCE"]
# Assurance Vie (VIE, PREVOYANCE) n'opère qu'en zone UEMOA ; le Ghana n'a que l'IARD.
PRODUCT_LINES_GH: list[str] = ["IARD_AUTO", "IARD_HABITATION", "IARD_SANTE"]
CLAIM_SUBMISSION_STATUSES: list[str] = ["PENDING", "APPROVED", "REJECTED"]
CLAIM_SUBMISSION_PROBS: list[float] = [0.5, 0.3, 0.2]

# --- Mobile money (A.6) -------------------------------------------------------
MM_PAYMENT_TYPES: list[str] = ["P2P", "MERCHANT_PAYMENT", "BILL_PAYMENT", "AIRTIME",
                               "CROSS_BORDER_TRANSFER"]
MM_PAYMENT_PROBS: list[float] = [0.40, 0.25, 0.15, 0.15, 0.05]
MM_OPERATORS: list[str] = ["WABA_PAY", "ORANGE_MONEY_PARTNER", "MTN_PARTNER"]
MM_OPERATOR_PROBS: list[float] = [0.5, 0.3, 0.2]
MM_STATUSES: list[str] = ["SUCCESS", "FAILED", "PENDING"]
MM_STATUS_PROBS: list[float] = [0.94, 0.04, 0.02]
MM_FEE_RATES: dict[str, float] = {"P2P": 0.01, "MERCHANT_PAYMENT": 0.0,
                                  "BILL_PAYMENT": 0.005, "AIRTIME": 0.0,
                                  "CROSS_BORDER_TRANSFER": 0.02}

# --- Remboursements de crédit (A.7) ------------------------------------------
LOAN_TYPES_BY_ENTITY: dict[str, list[str]] = {
    "BANK": ["CONSUMER", "MORTGAGE", "SME"],
    "MICROFINANCE": ["MICROCREDIT", "AGRICULTURAL"],
}
LOAN_TYPE_WEIGHTS: dict[str, list[float]] = {
    "BANK": [0.55, 0.15, 0.30],          # CONSUMER, MORTGAGE, SME
    "MICROFINANCE": [0.65, 0.35],        # MICROCREDIT, AGRICULTURAL
}
REPAYMENT_STATUSES: list[str] = ["ON_TIME", "LATE", "DEFAULT"]
REPAYMENT_PROBS: list[float] = [0.82, 0.12, 0.06]   # moyenne groupe (énoncé A.7)
# Taux de défaut par pays : NPL réaliste entre 3 % et 8 % (seuil BCEAO : 5 %).
DEFAULT_RATE: dict[str, float] = {
    "CI": 0.040, "SN": 0.050, "ML": 0.065, "BF": 0.060,
    "GN": 0.070, "TG": 0.055, "BJ": 0.045, "GH": 0.055,
}
LATE_RATE: float = 0.12

# --- Colonnes des fichiers CSV (ordre = contrat avec les jobs Spark) ---------
COLUMNS: dict[str, list[str]] = {
    "customers": ["customer_id", "country_code", "entity_type", "segment", "kyc_level",
                  "onboarding_date", "region", "is_active"],
    # entity_type & product_id ajoutés au schéma A.2 : contrainte « toutes les tables
    # portent country_code et entity_type » + jointure products en couche Silver.
    "accounts": ["account_id", "customer_id", "country_code", "entity_type",
                 "account_type", "product_id", "currency", "balance", "credit_limit",
                 "opened_date", "status"],
    "branches": ["branch_id", "country_code", "entity_type", "city", "region",
                 "branch_type", "is_active"],
    "products": ["product_id", "product_name", "product_category", "entity_type",
                 "country_code", "currency", "interest_rate", "monthly_fee",
                 "launch_date", "is_active"],
    "bank_transactions": ["transaction_id", "timestamp", "account_id",
                          "beneficiary_account", "branch_id", "country_code",
                          "transaction_type", "amount", "currency", "channel",
                          "transaction_status", "fee_amount", "entity_type"],
    "insurance_operations": ["operation_id", "timestamp", "customer_id", "account_id",
                             "country_code", "operation_type", "product_line", "amount",
                             "currency", "claim_status", "processing_days",
                             "entity_type"],
    "mobile_money_payments": ["payment_id", "timestamp", "sender_id", "receiver_id",
                              "sender_country", "receiver_country", "amount", "currency",
                              "payment_type", "operator", "status", "fee_amount",
                              "entity_type"],
    "loan_repayments": ["repayment_id", "timestamp", "loan_account_id", "customer_id",
                        "country_code", "amount_due", "amount_paid", "currency",
                        "due_date", "payment_date", "days_overdue", "loan_type",
                        "repayment_status", "entity_type"],
}

# Datasets transactionnels : nom logique -> préfixe de fichier (nomenclature 1.1)
EVENT_DATASETS: dict[str, str] = {
    "bank_transactions": "bank_txn",
    "insurance_operations": "insurance_ops",
    "mobile_money_payments": "mobile_money",
    "loan_repayments": "loan_repayments",
}
REFERENTIALS: list[str] = ["customers", "accounts", "branches", "products"]

# Volumes par défaut (énoncé 1.1 et nomenclature)
DEFAULT_ROWS: dict[str, int] = {
    "bank_transactions": 10_000,
    "insurance_operations": 5_000,
    "mobile_money_payments": 20_000,
    "loan_repayments": 5_000,
}
DEFAULT_REFERENTIAL_SIZES: dict[str, int] = {
    "customers": 500_000, "accounts": 800_000, "branches": 200, "products": 50,
}
