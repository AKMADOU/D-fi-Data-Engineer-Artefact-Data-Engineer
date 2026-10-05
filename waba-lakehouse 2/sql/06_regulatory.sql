-- =============================================================================
-- 06 — Reporting réglementaire J+1 (dag_regulatory_report)
-- =============================================================================

-- Dates de reporting disponibles
SELECT report_date, count(*) AS pays, count_if(npl_breach) AS pays_npl_au_dessus_du_seuil
FROM iceberg.gold.regulatory_bceao_daily GROUP BY report_date ORDER BY report_date DESC;

-- Déclaration BCEAO de la dernière date
SELECT country_code, nb_bank_transactions, bank_amount_eur, nb_high_value_transactions,
       nb_mobile_money_transactions, cross_border_outflows_eur, total_deposits_eur,
       total_loans_outstanding_eur, round(npl_ratio * 100, 2) AS npl_pct, npl_breach
FROM iceberg.gold.regulatory_bceao_daily
WHERE report_date = (SELECT max(report_date) FROM iceberg.gold.regulatory_bceao_daily)
ORDER BY country_code;

-- Déclaration CIMA (cumul du mois à date)
SELECT country_code, line_family, product_line, premiums_mtd_eur, claims_paid_mtd_eur,
       round(loss_ratio_mtd * 100, 1) AS loss_ratio_pct, loss_ratio_alert, nb_claims_pending
FROM iceberg.gold.regulatory_cima_daily
WHERE report_date = (SELECT max(report_date) FROM iceberg.gold.regulatory_cima_daily)
ORDER BY country_code, product_line;
