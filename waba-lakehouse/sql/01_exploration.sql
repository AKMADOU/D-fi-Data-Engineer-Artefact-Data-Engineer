-- =============================================================================
-- 01 — Exploration du catalogue Iceberg via Trino
-- Exécution : make trino-sql FILE=01_exploration.sql   (ou copier-coller dans make trino)
-- =============================================================================

SHOW SCHEMAS FROM iceberg;

SHOW TABLES FROM iceberg.bronze;

-- Les 8 tables bronze.* (raw.* au Level 1) et leur volumétrie
SELECT 'customers' AS table_name, count(*) AS row_count FROM iceberg.bronze.customers
UNION ALL SELECT 'accounts', count(*) FROM iceberg.bronze.accounts
UNION ALL SELECT 'branches', count(*) FROM iceberg.bronze.branches
UNION ALL SELECT 'products', count(*) FROM iceberg.bronze.products
UNION ALL SELECT 'bank_transactions', count(*) FROM iceberg.bronze.bank_transactions
UNION ALL SELECT 'insurance_operations', count(*) FROM iceberg.bronze.insurance_operations
UNION ALL SELECT 'mobile_money_payments', count(*) FROM iceberg.bronze.mobile_money_payments
UNION ALL SELECT 'loan_repayments', count(*) FROM iceberg.bronze.loan_repayments
ORDER BY table_name;

-- Schéma d'une table (country_code et entity_type présents partout)
DESCRIBE iceberg.bronze.bank_transactions;

-- Partitionnement Iceberg Bronze : country_code + jour d'ingestion (_ingested_at)
SHOW CREATE TABLE iceberg.bronze.bank_transactions;

SELECT "partition", record_count, file_count
FROM iceberg.bronze."bank_transactions$partitions"
ORDER BY record_count DESC
LIMIT 10;
