"""Contrat de données : schémas explicites et règles de validation par dataset.

C'est l'unique source de vérité côté Spark : le DDL Iceberg, la lecture CSV et
la validation sont tous dérivés de ces définitions (pas de dérive possible).
"""
from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql.types import (BooleanType, DateType, DecimalType, IntegerType, StringType,
                               StructField, StructType, TimestampType)

COUNTRIES = ("CI", "SN", "ML", "BF", "GN", "TG", "BJ", "GH")
UEMOA = ("CI", "SN", "ML", "BF", "GN", "TG", "BJ")
CURRENCIES = ("XOF", "GHS")
ENTITY_TYPES = ("BANK", "INSURANCE", "MOBILE_MONEY", "MICROFINANCE")
UUID_REGEX = r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"

# Montants : DECIMAL(20,2) plutôt que FLOAT, pour des agrégats financiers exacts.
MONEY = DecimalType(20, 2)
CORRUPT_COL = "_corrupt_record"


def _s(name: str, dtype=StringType()) -> StructField:
    return StructField(name, dtype, True)


@dataclass(frozen=True)
class DatasetSpec:
    name: str                     # nom de la table raw.<name>
    key: str                      # clé d'idempotence
    fields: tuple[StructField, ...]
    kind: str                     # "event" | "referential"
    required: tuple[str, ...]
    enums: dict[str, tuple[str, ...]] = field(default_factory=dict)
    non_negative: tuple[str, ...] = ()
    country_col: str = "country_code"   # pays servant au contrôle de devise
    currency_col: str | None = "currency"
    key_regex: str | None = None
    # Transformations de partitionnement Iceberg (partitionnement « caché »).
    partitioning: tuple[str, ...] = ("country_code",)

    @property
    def schema(self) -> StructType:
        return StructType(list(self.fields))

    @property
    def read_schema(self) -> StructType:
        """Schéma de lecture CSV : + colonne des lignes malformées (mode PERMISSIVE)."""
        return StructType([*self.fields, StructField(CORRUPT_COL, StringType(), True)])

    @property
    def table(self) -> str:
        """Table Level 1 (schéma raw)."""
        return f"raw.{self.name}"

    def table_in(self, namespace: str) -> str:
        return f"{namespace}.{self.name}"

    def partitioning_for(self, namespace: str) -> tuple[str, ...]:
        """raw (L1) : pays + date métier ; bronze (L2) : pays + date d'ingestion."""
        return BRONZE_PARTITIONING if namespace == "bronze" else self.partitioning

    def landing_prefixes(self, countries: tuple[str, ...] = COUNTRIES) -> list[str]:
        if self.kind == "referential":
            return [f"referentials/{self.name}/"]
        return [f"{cc}/{self.name}/" for cc in countries]


EVENT_PARTITIONING = ("country_code", "days(timestamp)")
# Couche Bronze (énoncé 2.2) : « partitionnement par country_code et date d'ingestion »
BRONZE_PARTITIONING = ("country_code", "days(_ingested_at)")
INGEST_NAMESPACES = ("bronze", "raw")

DATASETS: dict[str, DatasetSpec] = {spec.name: spec for spec in [
    # ---------------------------------------------------------------- référentiels
    DatasetSpec(
        name="customers", key="customer_id", kind="referential",
        fields=(_s("customer_id"), _s("country_code"), _s("entity_type"), _s("segment"),
                _s("kyc_level"), _s("onboarding_date", DateType()), _s("region"),
                _s("is_active", BooleanType())),
        required=("customer_id", "country_code", "entity_type", "segment", "kyc_level",
                  "onboarding_date", "is_active"),
        enums={"segment": ("RETAIL", "SME", "CORPORATE", "PREMIUM"),
               "kyc_level": ("BASIC", "STANDARD", "ENHANCED")},
        currency_col=None, key_regex=r"^WABA-[A-Z]{2}-C-\d{6,}$"),
    DatasetSpec(
        name="accounts", key="account_id", kind="referential",
        fields=(_s("account_id"), _s("customer_id"), _s("country_code"), _s("entity_type"),
                _s("account_type"), _s("product_id"), _s("currency"), _s("balance", MONEY),
                _s("credit_limit", MONEY), _s("opened_date", DateType()), _s("status")),
        required=("account_id", "customer_id", "country_code", "entity_type", "account_type",
                  "currency", "balance", "opened_date", "status"),
        enums={"account_type": ("CURRENT", "SAVINGS", "LOAN", "MOBILE_WALLET",
                                "INSURANCE_POLICY"),
               "status": ("ACTIVE", "FROZEN", "CLOSED", "DORMANT")},
        non_negative=("credit_limit",),  # balance < 0 autorisé (encours de prêt)
        key_regex=r"^WABA-[A-Z]{2}-A-\d{7,}$"),
    DatasetSpec(
        name="branches", key="branch_id", kind="referential",
        fields=(_s("branch_id"), _s("country_code"), _s("entity_type"), _s("city"),
                _s("region"), _s("branch_type"), _s("is_active", BooleanType())),
        required=("branch_id", "country_code", "entity_type", "city", "branch_type"),
        enums={"branch_type": ("FULL_SERVICE", "DIGITAL_ONLY", "AGENCY_BANKING",
                               "ATM_POINT")},
        currency_col=None, key_regex=r"^WABA-[A-Z]{2}-B-\d{3,}$"),
    DatasetSpec(
        name="products", key="product_id", kind="referential",
        fields=(_s("product_id"), _s("product_name"), _s("product_category"),
                _s("entity_type"), _s("country_code"), _s("currency"),
                _s("interest_rate", DecimalType(6, 2)), _s("monthly_fee", MONEY),
                _s("launch_date", DateType()), _s("is_active", BooleanType())),
        required=("product_id", "product_name", "product_category", "entity_type",
                  "country_code", "currency"),
        non_negative=("interest_rate", "monthly_fee"),
        key_regex=r"^WABA-[A-Z]{2}-P-\d{3,}$"),
    # ------------------------------------------------------------------- événements
    DatasetSpec(
        name="bank_transactions", key="transaction_id", kind="event",
        fields=(_s("transaction_id"), _s("timestamp", TimestampType()), _s("account_id"),
                _s("beneficiary_account"), _s("branch_id"), _s("country_code"),
                _s("transaction_type"), _s("amount", MONEY), _s("currency"), _s("channel"),
                _s("transaction_status"), _s("fee_amount", MONEY), _s("entity_type")),
        required=("transaction_id", "timestamp", "account_id", "branch_id", "country_code",
                  "transaction_type", "amount", "currency", "transaction_status",
                  "entity_type"),
        enums={"transaction_type": ("TRANSFER", "PAYMENT", "WITHDRAWAL", "DEPOSIT",
                                    "INTERNATIONAL_WIRE"),
               "channel": ("BRANCH", "ATM", "MOBILE_APP", "INTERNET_BANKING", "USSD"),
               "transaction_status": ("SUCCESS", "FAILED", "REVERSED")},
        non_negative=("amount", "fee_amount"), key_regex=UUID_REGEX,
        partitioning=EVENT_PARTITIONING),
    DatasetSpec(
        name="insurance_operations", key="operation_id", kind="event",
        fields=(_s("operation_id"), _s("timestamp", TimestampType()), _s("customer_id"),
                _s("account_id"), _s("country_code"), _s("operation_type"),
                _s("product_line"), _s("amount", MONEY), _s("currency"), _s("claim_status"),
                _s("processing_days", IntegerType()), _s("entity_type")),
        required=("operation_id", "timestamp", "customer_id", "account_id", "country_code",
                  "operation_type", "product_line", "amount", "currency", "entity_type"),
        enums={"operation_type": ("PREMIUM_PAYMENT", "CLAIM_SUBMISSION", "CLAIM_PAYMENT",
                                  "POLICY_RENEWAL", "POLICY_CANCELLATION"),
               "product_line": ("VIE", "IARD_AUTO", "IARD_HABITATION", "IARD_SANTE",
                                "PREVOYANCE"),
               "claim_status": ("PENDING", "APPROVED", "REJECTED", "PAID")},
        non_negative=("amount", "processing_days"), key_regex=UUID_REGEX,
        partitioning=EVENT_PARTITIONING),
    DatasetSpec(
        name="mobile_money_payments", key="payment_id", kind="event",
        fields=(_s("payment_id"), _s("timestamp", TimestampType()), _s("sender_id"),
                _s("receiver_id"), _s("sender_country"), _s("receiver_country"),
                _s("amount", MONEY), _s("currency"), _s("payment_type"), _s("operator"),
                _s("status"), _s("fee_amount", MONEY), _s("entity_type")),
        required=("payment_id", "timestamp", "sender_id", "receiver_id", "sender_country",
                  "receiver_country", "amount", "currency", "payment_type", "status",
                  "entity_type"),
        enums={"payment_type": ("P2P", "MERCHANT_PAYMENT", "BILL_PAYMENT", "AIRTIME",
                                "CROSS_BORDER_TRANSFER"),
               "operator": ("WABA_PAY", "ORANGE_MONEY_PARTNER", "MTN_PARTNER"),
               "status": ("SUCCESS", "FAILED", "PENDING"),
               "receiver_country": COUNTRIES},
        non_negative=("amount", "fee_amount"), country_col="sender_country",
        key_regex=UUID_REGEX, partitioning=EVENT_PARTITIONING),
    DatasetSpec(
        name="loan_repayments", key="repayment_id", kind="event",
        fields=(_s("repayment_id"), _s("timestamp", TimestampType()), _s("loan_account_id"),
                _s("customer_id"), _s("country_code"), _s("amount_due", MONEY),
                _s("amount_paid", MONEY), _s("currency"), _s("due_date", DateType()),
                _s("payment_date", DateType()), _s("days_overdue", IntegerType()),
                _s("loan_type"), _s("repayment_status"), _s("entity_type")),
        required=("repayment_id", "timestamp", "loan_account_id", "customer_id",
                  "country_code", "amount_due", "amount_paid", "currency", "due_date",
                  "days_overdue", "loan_type", "repayment_status", "entity_type"),
        enums={"loan_type": ("CONSUMER", "MORTGAGE", "SME", "AGRICULTURAL", "MICROCREDIT"),
               "repayment_status": ("ON_TIME", "LATE", "DEFAULT")},
        non_negative=("amount_due", "amount_paid", "days_overdue"), key_regex=UUID_REGEX,
        partitioning=EVENT_PARTITIONING),
]}

REFERENTIAL_DATASETS = [n for n, s in DATASETS.items() if s.kind == "referential"]
EVENT_DATASETS = [n for n, s in DATASETS.items() if s.kind == "event"]

# Colonnes techniques ajoutées à chaque table raw (traçabilité / lineage).
METADATA_FIELDS = (
    StructField("_source_file", StringType(), False),
    StructField("_batch_id", StringType(), False),
    StructField("_ingested_at", TimestampType(), False),
)
