-- =============================================================================
-- 05 — KPIs Gold (7 tables), filtrables par pays
-- make trino-sql FILE=05_gold_kpis.sql      (remplacer 'CI' pour changer de pays)
-- =============================================================================

-- 1. Volume quotidien des transactions (banque + mobile money)
SELECT txn_date, country_code, entity_type, transaction_type, nb_transactions,
       total_amount_eur, failure_rate
FROM iceberg.gold.daily_transaction_volume
WHERE country_code = 'CI'
ORDER BY txn_date DESC, total_amount_eur DESC
LIMIT 20;

-- 2. Taux de créances douteuses (NPL) — seuil BCEAO 5 %
SELECT country_code, loan_type, nb_loans, nb_loans_npl, outstanding_total_eur,
       outstanding_npl_eur, round(npl_ratio * 100, 2) AS npl_pct, is_above_threshold
FROM iceberg.gold.npl_ratio_by_country
ORDER BY country_code, loan_type = 'ALL' DESC, loan_type;

-- 3. ARPC mensuel par pays et segment
SELECT month, country_code, customer_segment, active_customers, commissions_eur, interest_eur,
       revenue_eur, arpc_eur
FROM iceberg.gold.customer_arpu_monthly
WHERE country_code IN ('CI', 'SN')
ORDER BY month, country_code, arpc_eur DESC;

-- 4. Loss ratio par produit — seuil CIMA 70 %
SELECT month, country_code, product_line, premiums_eur, claims_paid_eur,
       round(loss_ratio * 100, 1) AS loss_ratio_pct, is_above_threshold
FROM iceberg.gold.loss_ratio_by_product
ORDER BY month, country_code, product_line;

-- Vue consolidée trimestre par pays (plus stable que le grain mensuel x produit)
SELECT country_code, sum(claims_paid_eur) / sum(premiums_eur) AS loss_ratio_trimestre
FROM iceberg.gold.loss_ratio_by_product GROUP BY country_code ORDER BY country_code;

-- 5. Délai de traitement des sinistres (jours ouvrés) par ligne IARD / VIE
SELECT month, country_code, line_family, nb_claims, avg_processing_days,
       median_processing_days, p90_processing_days, nb_pending
FROM iceberg.gold.claims_processing_time
ORDER BY month, country_code, line_family;

-- 6. Flux journalier mobile money
SELECT transaction_date, country_code, nb_transactions, amount_total_eur,
       round(failure_rate * 100, 2) AS taux_echec_pct, active_users, fees_eur
FROM iceberg.gold.mobile_money_daily_flow
WHERE country_code = 'CI'
ORDER BY transaction_date DESC LIMIT 15;

-- 7. Transferts transfrontaliers UEMOA : corridors et évolution hebdomadaire
SELECT week_start, corridor, channel, nb_transfers, amount_total_eur, avg_amount_eur,
       wow_change_pct
FROM iceberg.gold.cross_border_transfers
WHERE is_uemoa_corridor
ORDER BY week_start DESC, amount_total_eur DESC LIMIT 20;

-- Flux entrants / sortants par pays
SELECT country, sum(sortants_eur) AS sortants_eur, sum(entrants_eur) AS entrants_eur
FROM (
    SELECT sender_country AS country, amount_total_eur AS sortants_eur, 0 AS entrants_eur
    FROM iceberg.gold.cross_border_transfers
    UNION ALL
    SELECT receiver_country, 0, amount_total_eur FROM iceberg.gold.cross_border_transfers
) GROUP BY country ORDER BY country;
