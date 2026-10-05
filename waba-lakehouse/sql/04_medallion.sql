-- =============================================================================
-- 04 — Architecture Médaillon : les 3 zones, volumétrie, qualité Silver
-- make trino-sql FILE=04_medallion.sql
-- =============================================================================

-- 1. Les zones existent et contiennent des données distinctes
SHOW SCHEMAS FROM iceberg;                       -- bronze, silver, gold, audit (+ raw au L1)

SELECT table_schema AS zone, count(*) AS nb_tables
FROM iceberg.information_schema.tables
WHERE table_schema IN ('bronze', 'silver', 'gold')
GROUP BY table_schema ORDER BY zone;

-- 2. Bronze vs Silver : lignes conservées / mises en quarantaine
SELECT 'bank_transactions' AS dataset,
       (SELECT count(*) FROM iceberg.bronze.bank_transactions)  AS bronze,
       (SELECT count(*) FROM iceberg.silver.bank_transactions)  AS silver
UNION ALL SELECT 'insurance_operations',
       (SELECT count(*) FROM iceberg.bronze.insurance_operations),
       (SELECT count(*) FROM iceberg.silver.insurance_operations)
UNION ALL SELECT 'mobile_money_payments',
       (SELECT count(*) FROM iceberg.bronze.mobile_money_payments),
       (SELECT count(*) FROM iceberg.silver.mobile_money_payments)
UNION ALL SELECT 'loan_repayments',
       (SELECT count(*) FROM iceberg.bronze.loan_repayments),
       (SELECT count(*) FROM iceberg.silver.loan_repayments)
UNION ALL SELECT 'accounts',
       (SELECT count(*) FROM iceberg.bronze.accounts),
       (SELECT count(*) FROM iceberg.silver.accounts);

-- 3. Partitionnement par country_code dans chaque zone
SELECT 'bronze' AS zone, "partition".country_code AS country_code, sum(record_count) AS lignes
FROM iceberg.bronze."bank_transactions$partitions" GROUP BY 1, 2
UNION ALL
SELECT 'silver', "partition".country_code, sum(record_count)
FROM iceberg.silver."bank_transactions$partitions" GROUP BY 1, 2
UNION ALL
SELECT 'gold', "partition".country_code, sum(record_count)
FROM iceberg.gold."daily_transaction_volume$partitions" GROUP BY 1, 2
ORDER BY zone, country_code;

-- 4. Qualité Silver : unicité des clés (attendu : 0 partout)
SELECT 'bank_transactions' AS table_name, count(*) - count(DISTINCT transaction_id) AS doublons
FROM iceberg.silver.bank_transactions
UNION ALL SELECT 'insurance_operations', count(*) - count(DISTINCT operation_id)
FROM iceberg.silver.insurance_operations
UNION ALL SELECT 'mobile_money_payments', count(*) - count(DISTINCT payment_id)
FROM iceberg.silver.mobile_money_payments
UNION ALL SELECT 'loan_repayments', count(*) - count(DISTINCT repayment_id)
FROM iceberg.silver.loan_repayments
UNION ALL SELECT 'customers', count(*) - count(DISTINCT customer_key) FROM iceberg.silver.customers
UNION ALL SELECT 'accounts', count(*) - count(DISTINCT account_key) FROM iceberg.silver.accounts;

-- 5. Conversion EUR : taux appliqués (XOF parité fixe, GHS série mensuelle)
SELECT currency, rate_month, eur_per_unit, 1 / eur_per_unit AS unites_pour_1_eur, source
FROM iceberg.silver.fx_rates
-- trimestre précédent (celui que produit le générateur)
WHERE rate_month >= date_trunc('quarter', current_date) - INTERVAL '3' MONTH
  AND rate_month < date_trunc('quarter', current_date)
ORDER BY currency, rate_month;

SELECT currency, count(*) AS nb, sum(amount) AS montant_local, sum(amount_eur) AS montant_eur
FROM iceberg.silver.bank_transactions GROUP BY currency;

-- 6. Pseudonymisation : aucun identifiant client/compte en clair en Silver
SELECT table_name, column_name
FROM iceberg.information_schema.columns
WHERE table_schema = 'silver'
  AND column_name IN ('customer_id', 'account_id', 'sender_id', 'receiver_id',
                      'beneficiary_account', 'loan_account_id');   -- attendu : 0 ligne

-- 7. Quarantaine (clés orphelines) et montants aberrants signalés
SELECT dataset, reason, count(*) AS nb FROM iceberg.audit.silver_quarantine
GROUP BY dataset, reason ORDER BY dataset, nb DESC;

SELECT country_code, count_if(is_aberrant) AS aberrants, count_if(is_high_value) AS montants_eleves
FROM iceberg.silver.bank_transactions GROUP BY country_code ORDER BY country_code;
