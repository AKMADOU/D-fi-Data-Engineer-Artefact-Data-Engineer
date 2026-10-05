-- =============================================================================
-- Level 3 — Architecture Lambda : batch (Iceberg Gold) + temps réel (Kafka) dans Trino
--   ./scripts/trino.sh -f 07_lambda.sql      (ou make lambda)
-- =============================================================================

-- 1) Requête cible de l'énoncé, telle quelle.
--    gold.daily_transaction_volume = couche batch (Iceberg) ;
--    kafka.default."silver-bank-transactions" = couche vitesse (topic Kafka, JSON décrit
--    dans trino/etc/kafka/silver-bank-transactions.json : event_time, streaming_amount_eur).
SELECT
    COALESCE(b.country_code, s.country_code) AS country,
    COALESCE(b.txn_date, CAST(s.event_time AS DATE)) AS date,
    COALESCE(b.total_amount_eur, 0) + COALESCE(s.streaming_amount_eur, 0) AS total_eur
FROM gold.daily_transaction_volume b
FULL OUTER JOIN kafka.default."silver-bank-transactions" s
    ON b.country_code = s.country_code
    AND b.txn_date = CAST(s.event_time AS DATE)
ORDER BY 1, 2;

-- 2) Variante « production » : la requête ci-dessus joint un agrégat journalier à des
--    messages unitaires (le montant batch est répété pour chaque message du jour).
--    Ici les deux côtés sont agrégés par (pays, jour) avant la jointure, et seuls les
--    événements pas encore traités par le batch sont pris côté Kafka (jours > dernier
--    jour présent en Gold pour le pays) : pas de double comptage.
WITH batch AS (
    SELECT country_code, txn_date, sum(total_amount_eur) AS batch_eur
    FROM gold.daily_transaction_volume
    GROUP BY 1, 2
),
last_batch AS (
    SELECT country_code, max(txn_date) AS last_day FROM batch GROUP BY 1
),
speed AS (
    SELECT s.country_code, CAST(s.event_time AS DATE) AS txn_date,
           sum(s.streaming_amount_eur) AS speed_eur, count(*) AS speed_events
    FROM kafka.default."silver-bank-transactions" s
    LEFT JOIN last_batch l ON l.country_code = s.country_code
    WHERE s.is_success AND NOT s.is_aberrant
      AND (l.last_day IS NULL OR CAST(s.event_time AS DATE) > l.last_day)
    GROUP BY 1, 2
)
SELECT COALESCE(b.country_code, s.country_code) AS country,
       COALESCE(b.txn_date, s.txn_date)         AS date,
       round(COALESCE(b.batch_eur, 0), 2)        AS batch_eur,
       round(COALESCE(s.speed_eur, 0), 2)        AS realtime_eur,
       COALESCE(s.speed_events, 0)               AS realtime_events,
       round(COALESCE(b.batch_eur, 0) + COALESCE(s.speed_eur, 0), 2) AS total_eur
FROM batch b
FULL OUTER JOIN speed s ON b.country_code = s.country_code AND b.txn_date = s.txn_date
ORDER BY 2 DESC, 1
LIMIT 50;
