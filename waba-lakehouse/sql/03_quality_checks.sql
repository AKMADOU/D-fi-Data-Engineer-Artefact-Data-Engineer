-- =============================================================================
-- 03 — Contrôles qualité : cohérence référentielle, idempotence, rejets, audit
-- Chaque contrôle doit retourner 0 anomalie.
-- =============================================================================

-- 1. Cohérence référentielle : clés orphelines (attendu : 0 partout)
SELECT 'bank.account_id' AS controle, count(*) AS orphelins
FROM iceberg.bronze.bank_transactions t
LEFT JOIN iceberg.bronze.accounts a ON a.account_id = t.account_id
WHERE a.account_id IS NULL
UNION ALL
SELECT 'bank.beneficiary_account', count(*)
FROM iceberg.bronze.bank_transactions t
LEFT JOIN iceberg.bronze.accounts a ON a.account_id = t.beneficiary_account
WHERE t.beneficiary_account IS NOT NULL AND a.account_id IS NULL
UNION ALL
SELECT 'bank.branch_id', count(*)
FROM iceberg.bronze.bank_transactions t
LEFT JOIN iceberg.bronze.branches b ON b.branch_id = t.branch_id
WHERE b.branch_id IS NULL
UNION ALL
SELECT 'insurance.customer_id', count(*)
FROM iceberg.bronze.insurance_operations o
LEFT JOIN iceberg.bronze.customers c ON c.customer_id = o.customer_id
WHERE c.customer_id IS NULL
UNION ALL
SELECT 'insurance.account_id', count(*)
FROM iceberg.bronze.insurance_operations o
LEFT JOIN iceberg.bronze.accounts a ON a.account_id = o.account_id
WHERE a.account_id IS NULL
UNION ALL
SELECT 'mobile_money.sender_id', count(*)
FROM iceberg.bronze.mobile_money_payments p
LEFT JOIN iceberg.bronze.customers c ON c.customer_id = p.sender_id
WHERE c.customer_id IS NULL
UNION ALL
SELECT 'mobile_money.receiver_id', count(*)
FROM iceberg.bronze.mobile_money_payments p
LEFT JOIN iceberg.bronze.customers c ON c.customer_id = p.receiver_id
WHERE c.customer_id IS NULL
UNION ALL
SELECT 'loan.loan_account_id (type LOAN)', count(*)
FROM iceberg.bronze.loan_repayments r
LEFT JOIN iceberg.bronze.accounts a
       ON a.account_id = r.loan_account_id AND a.account_type = 'LOAN'
WHERE a.account_id IS NULL
UNION ALL
SELECT 'loan.customer_id', count(*)
FROM iceberg.bronze.loan_repayments r
LEFT JOIN iceberg.bronze.customers c ON c.customer_id = r.customer_id
WHERE c.customer_id IS NULL
UNION ALL
SELECT 'accounts.customer_id', count(*)
FROM iceberg.bronze.accounts a
LEFT JOIN iceberg.bronze.customers c ON c.customer_id = a.customer_id
WHERE c.customer_id IS NULL;

-- 2. Idempotence : aucune clé métier en double (attendu : 0)
SELECT 'bank_transactions' AS table_name, count(*) - count(DISTINCT transaction_id) AS doublons
FROM iceberg.bronze.bank_transactions
UNION ALL SELECT 'insurance_operations', count(*) - count(DISTINCT operation_id)
FROM iceberg.bronze.insurance_operations
UNION ALL SELECT 'mobile_money_payments', count(*) - count(DISTINCT payment_id)
FROM iceberg.bronze.mobile_money_payments
UNION ALL SELECT 'loan_repayments', count(*) - count(DISTINCT repayment_id)
FROM iceberg.bronze.loan_repayments
UNION ALL SELECT 'customers', count(*) - count(DISTINCT customer_id) FROM iceberg.bronze.customers
UNION ALL SELECT 'accounts', count(*) - count(DISTINCT account_id) FROM iceberg.bronze.accounts;

-- 3. Devise conforme au pays (XOF en UEMOA, GHS au Ghana) (attendu : 0)
SELECT 'bank_transactions' AS table_name, count(*) AS incoherences
FROM iceberg.bronze.bank_transactions
WHERE currency <> CASE WHEN country_code = 'GH' THEN 'GHS' ELSE 'XOF' END
UNION ALL
SELECT 'accounts', count(*) FROM iceberg.bronze.accounts
WHERE currency <> CASE WHEN country_code = 'GH' THEN 'GHS' ELSE 'XOF' END;

-- 4. Lignes rejetées par motif (validation Spark)
SELECT dataset, reject_reason, count(*) AS nb
FROM iceberg.audit.rejected_records
GROUP BY dataset, reject_reason
ORDER BY dataset, nb DESC;

-- 5. Journal d'ingestion : derniers lots
SELECT dataset, batch_id, status, count(*) AS fichiers,
       sum(rows_read) AS lues, sum(rows_valid) AS valides, sum(rows_rejected) AS rejetees,
       max(processed_at) AS traite_le
FROM iceberg.audit.ingestion_log
GROUP BY dataset, batch_id, status
ORDER BY traite_le DESC
LIMIT 20;

-- 6. Historique des commits Iceberg : un rejeu (idempotent) n'ajoute aucune ligne
SELECT committed_at, snapshot_id, operation,
       summary['added-records']  AS lignes_ajoutees,
       summary['total-records']  AS total
FROM iceberg.bronze."bank_transactions$snapshots"
ORDER BY committed_at;

-- 7. Time travel : état de la table au premier snapshot
-- SELECT count(*) FROM iceberg.bronze.bank_transactions FOR VERSION AS OF <snapshot_id>;
