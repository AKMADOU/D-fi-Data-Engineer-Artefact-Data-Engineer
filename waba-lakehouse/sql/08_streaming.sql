-- =============================================================================
-- Level 3 — contrôles du pipeline temps réel (make streaming-check)
-- =============================================================================

-- 1) NiFi -> Kafka : volume et lag par topic raw-* (critère : lag < 30 s)
--    lag = horodatage Kafka du message - dépôt du fichier dans MinIO (landed_at)
SELECT 'raw-bank-transactions' AS topic, count(*) AS messages,
       max(date_diff('second', landed_at, _timestamp)) AS max_lag_s,
       approx_percentile(date_diff('second', landed_at, _timestamp), 0.95) AS p95_lag_s
FROM kafka.default."raw-bank-transactions" WHERE landed_at IS NOT NULL
UNION ALL
SELECT 'raw-insurance-operations', count(*), max(date_diff('second', landed_at, _timestamp)),
       approx_percentile(date_diff('second', landed_at, _timestamp), 0.95)
FROM kafka.default."raw-insurance-operations" WHERE landed_at IS NOT NULL
UNION ALL
SELECT 'raw-mobile-money-payments', count(*), max(date_diff('second', landed_at, _timestamp)),
       approx_percentile(date_diff('second', landed_at, _timestamp), 0.95)
FROM kafka.default."raw-mobile-money-payments" WHERE landed_at IS NOT NULL
UNION ALL
SELECT 'raw-loan-repayments', count(*), max(date_diff('second', landed_at, _timestamp)),
       approx_percentile(date_diff('second', landed_at, _timestamp), 0.95)
FROM kafka.default."raw-loan-repayments" WHERE landed_at IS NOT NULL;

-- 2) Job 1 : topics silver-* et tables Iceberg temps réel (fraîcheur)
SELECT 'kafka silver-bank-transactions' AS source, count(*) AS n, max(_silver_processed_at) AS last_update
FROM kafka.default."silver-bank-transactions"
UNION ALL
SELECT 'iceberg silver.rt_bank_transactions', count(*), max(_silver_processed_at)
FROM iceberg.silver.rt_bank_transactions
UNION ALL
SELECT 'iceberg silver.rt_mobile_money_payments', count(*), max(_silver_processed_at)
FROM iceberg.silver.rt_mobile_money_payments
UNION ALL
SELECT 'iceberg silver.rt_insurance_operations', count(*), max(_silver_processed_at)
FROM iceberg.silver.rt_insurance_operations;

-- 3) Job 2 : alertes par règle (les 3 règles de fraude + AML + liquidité)
SELECT 'gold-fraud-alerts' AS topic, rule, severity, count(*) AS alerts, max(detected_at) AS last
FROM kafka.default."gold-fraud-alerts" GROUP BY 1, 2, 3
UNION ALL
SELECT 'gold-aml-events', rule || ' (' || currency || ')', severity, count(*), max(detected_at)
FROM kafka.default."gold-aml-events" GROUP BY 1, 2, 3
UNION ALL
SELECT 'gold-liquidity-alerts', rule, severity, count(*), max(detected_at)
FROM kafka.default."gold-liquidity-alerts" GROUP BY 1, 2, 3
ORDER BY 1, 2;

-- 4) Dernières alertes (tables Iceberg Gold, historisées)
SELECT rule, country_code, subject_key, event_time, round(amount_eur, 2) AS amount_eur,
       threshold, details
FROM iceberg.gold.fraud_alerts ORDER BY detected_at DESC LIMIT 10;

-- 5) Dead Letter Queue : rien n'est ignoré silencieusement
SELECT stage, dataset, split_part(reject_reason, ';', 1) AS reason, count(*) AS messages
FROM kafka.default."dlq-financial-events"
GROUP BY 1, 2, 3 ORDER BY 4 DESC;
