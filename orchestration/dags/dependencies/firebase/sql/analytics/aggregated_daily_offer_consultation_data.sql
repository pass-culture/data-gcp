SELECT
    fe.event_date,
    o.offerer_id,
    o.offerer_name,
    o.venue_id,
    o.venue_name,
    o.offer_id,
    o.offer_name,
    c.name,
    fe.event_name,
    fe.traffic_medium,
    fe.traffic_campaign,
    fe.origin,
    CASE
        WHEN fe.user_id IS NOT NULL
        AND DATETIME(fe.event_timestamp) BETWEEN eud.user_deposit_creation_date
        AND eud.user_deposit_expiration_date THEN 'Bénéficiaire'
        WHEN fe.user_id IS NOT NULL
        AND DATETIME(fe.event_timestamp) > eud.user_deposit_expiration_date THEN 'Ancien bénéficiaire'
        WHEN fe.user_id IS NOT NULL THEN 'Non bénéficiaire'
        ELSE 'Non loggué'
    END AS user_role,
    IF(
        EXTRACT(
            DAYOFYEAR
            FROM
                fe.event_date
        ) < EXTRACT(
            DAYOFYEAR
            FROM
                eud.user_birth_date
        ),
        DATE_DIFF(fe.event_date, eud.user_birth_date, YEAR) - 1,
        DATE_DIFF(fe.event_date, eud.user_birth_date, YEAR)
    ) AS user_age,
    COUNT(*) AS cnt_events
FROM
    `{{ bigquery_analytics_dataset }}.firebase_events` fe
    JOIN `{{ bigquery_analytics_dataset }}.enriched_offer_data` o ON fe.offer_id = o.offer_id
    AND fe.event_name IN (
        'ConsultOffer',
        'ConsultWholeOffer',
        'ConsultDescriptionDetails'
    )
    LEFT JOIN `{{ bigquery_analytics_dataset }}.applicative_database_offer_criterion` oc ON oc.offerId = o.offer_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}.applicative_database_criterion` c ON oc.criterionId = c.id
    LEFT JOIN `{{ bigquery_analytics_dataset }}.enriched_user_data` eud ON fe.user_id = eud.user_id
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14