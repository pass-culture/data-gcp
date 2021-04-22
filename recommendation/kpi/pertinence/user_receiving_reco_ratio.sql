SELECT(
    ROUND(
        (
            SELECT COUNT(DISTINCT userid) FROM `passculture-data-prod.raw_prod.past_recommended_offers`
            WHERE date >= PARSE_TIMESTAMP('%Y%m%d', @DS_START_DATE)
            AND date <= PARSE_TIMESTAMP('%Y%m%d', @DS_END_DATE)
        ) / (
            SELECT COUNT(DISTINCT user_id_dehumanized)
            FROM `passculture-data-prod.clean_prod.matomo_visits`
            WHERE  visit_first_action_time >= PARSE_TIMESTAMP('%Y%m%d', @DS_START_DATE)
            AND visit_first_action_time <= PARSE_TIMESTAMP('%Y%m%d', @DS_END_DATE)
        ) * 100, 1
    )
) AS user_receiving_recommendations_ratio
