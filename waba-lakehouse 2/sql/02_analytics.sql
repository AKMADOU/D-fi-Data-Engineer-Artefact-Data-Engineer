-- =============================================================================
-- 02 — Requêtes analytiques de base (énoncé 1.4)
-- Les montants restent en devise locale (XOF / GHS) : on ne somme jamais deux
-- devises ensemble. La conversion EUR est une transformation Silver (Level 2).
-- =============================================================================

-- 1. Soldes par pays (comptes actifs, hors prêts dont le solde est un encours négatif)
SELECT country_code,
       currency,
       count(*)                                   AS nb_comptes,
       sum(balance)                               AS solde_total,
       round(avg(balance), 2)                     AS solde_moyen,
       approx_percentile(CAST(balance AS double), 0.5) AS solde_median
FROM iceberg.bronze.accounts
WHERE status = 'ACTIVE'
  AND account_type <> 'LOAN'
GROUP BY country_code, currency
ORDER BY country_code;

-- 2. Encours de crédit par pays et entité (banque vs microfinance)
SELECT country_code, entity_type, currency,
       count(*)             AS nb_prets,
       -sum(balance)        AS encours_total,
       sum(credit_limit)    AS montant_accorde
FROM iceberg.bronze.accounts
WHERE account_type = 'LOAN'
GROUP BY country_code, entity_type, currency
ORDER BY country_code, entity_type;

-- 3. Volumes de transactions bancaires par pays et par mois
SELECT country_code,
       date_trunc('month', "timestamp")                       AS mois,
       count(*)                                               AS nb_transactions,
       count_if(transaction_status = 'SUCCESS')               AS nb_reussies,
       round(100.0 * count_if(transaction_status = 'FAILED') / count(*), 2) AS pct_echec,
       sum(amount) FILTER (WHERE transaction_status = 'SUCCESS') AS montant_reussi,
       currency
FROM iceberg.bronze.bank_transactions
GROUP BY country_code, date_trunc('month', "timestamp"), currency
ORDER BY country_code, mois;

-- 4. Volumes par type de transaction et canal (un pays, pruning de partition)
SELECT transaction_type, channel, count(*) AS nb, sum(amount) AS montant_xof
FROM iceberg.bronze.bank_transactions
WHERE country_code = 'CI'
  AND "timestamp" >= TIMESTAMP '2026-04-01 00:00:00 UTC'
GROUP BY transaction_type, channel
ORDER BY nb DESC;

-- 5. Comptages par entité et par pays (clients, comptes)
SELECT c.entity_type,
       c.country_code,
       count(DISTINCT c.customer_id)  AS nb_clients,
       count(a.account_id)            AS nb_comptes
FROM iceberg.bronze.customers c
LEFT JOIN iceberg.bronze.accounts a ON a.customer_id = c.customer_id
GROUP BY c.entity_type, c.country_code
ORDER BY c.entity_type, c.country_code;

-- 6. Activité consolidée multi-entités (volumes d'opérations par pays et ligne métier)
SELECT country_code, entity_type, count(*) AS nb_operations
FROM (
    SELECT country_code, entity_type FROM iceberg.bronze.bank_transactions
    UNION ALL SELECT country_code, entity_type FROM iceberg.bronze.insurance_operations
    UNION ALL SELECT country_code, entity_type FROM iceberg.bronze.mobile_money_payments
    UNION ALL SELECT country_code, entity_type FROM iceberg.bronze.loan_repayments
) ops
GROUP BY country_code, entity_type
ORDER BY country_code, nb_operations DESC;

-- 7. Mobile money : flux transfrontaliers (corridors)
SELECT sender_country, receiver_country, count(*) AS nb, sum(amount) AS montant, currency
FROM iceberg.bronze.mobile_money_payments
WHERE payment_type = 'CROSS_BORDER_TRANSFER' AND status = 'SUCCESS'
GROUP BY sender_country, receiver_country, currency
ORDER BY nb DESC;

-- 8. Assurance : sinistralité par ligne produit
SELECT country_code, product_line,
       sum(amount) FILTER (WHERE operation_type = 'PREMIUM_PAYMENT') AS primes,
       sum(amount) FILTER (WHERE operation_type = 'CLAIM_PAYMENT')   AS sinistres_payes,
       avg(processing_days) FILTER (WHERE claim_status IS NOT NULL)  AS delai_moyen_jours
FROM iceberg.bronze.insurance_operations
GROUP BY country_code, product_line
ORDER BY country_code, product_line;

-- 9. Crédit : répartition des statuts de remboursement
SELECT country_code, entity_type, repayment_status, count(*) AS nb,
       round(100.0 * count(*) / sum(count(*)) OVER (PARTITION BY country_code, entity_type), 1)
           AS pct
FROM iceberg.bronze.loan_repayments
GROUP BY country_code, entity_type, repayment_status
ORDER BY country_code, entity_type, repayment_status;
