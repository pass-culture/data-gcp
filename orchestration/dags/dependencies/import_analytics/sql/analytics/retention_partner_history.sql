WITH all_activated_partners_and_days_since_activation AS
( -- Pour chaque partner_id, une ligne par jour depuis la 1ère offre publiée
SELECT
    partner_id,
    first_offer_creation_date,
    DATE_ADD(DATE('2022-07-01'), INTERVAL offset DAY) AS day -- Tous les jours depuis le 1er juillet (date à laquelle on a commencé à storer la réservabilité d'une offre / d'un lieu)
FROM `{{ bigquery_analytics_dataset }}`.enriched_cultural_partner_data
CROSS JOIN UNNEST(GENERATE_ARRAY(0, DATE_DIFF(CURRENT_DATE(), '2022-07-01', DAY))) AS offset
WHERE DATE_ADD(DATE('2022-07-01'), INTERVAL offset DAY) >= first_offer_creation_date -- Les jours depuis la 1ère offre
AND DATE_ADD(DATE('2022-07-01'), INTERVAL offset DAY) < CURRENT_DATE() -- Que des jours avant aujourd'hui
),

all_days_and_bookability AS (
SELECT
    all_activated_partners_and_days_since_activation.partner_id
    ,first_offer_creation_date
    ,day
    ,COALESCE(total_bookable_offers,0) AS total_bookable_offers
FROM all_activated_partners_and_days_since_activation
LEFT JOIN `{{ bigquery_analytics_dataset }}`.bookable_partner_history ON bookable_partner_history.partner_id = all_activated_partners_and_days_since_activation.partner_id
                                                AND bookable_partner_history.partition_date = all_activated_partners_and_days_since_activation.day
)
SELECT
    partner_id
    ,first_offer_creation_date
    ,day
    ,total_bookable_offers
    ,MAX(CASE WHEN total_bookable_offers != 0 THEN day END) OVER (PARTITION BY partner_id ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_bookable_date
    ,DATE_DIFF(day, MAX(CASE WHEN total_bookable_offers != 0 THEN day END) OVER (PARTITION BY partner_id ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), DAY) AS days_since_last_bookable_date
FROM all_days_and_bookability