{{
    config(
        **custom_incremental_config(
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'consultation_date', 'data_type': 'date'}
    )
) }}

SELECT
    consult.unique_session_id,
    consult.user_id,
    consult.consultation_date,
    COUNT(score.consultation_id) discovery_score,
    COUNT(CASE WHEN score.type = "item" THEN score.consultation_id END) AS item_discovery_score,
    COUNT(CASE WHEN score.type = "offer_category" THEN score.consultation_id END) AS offer_category_discovery_score,
    COUNT(CASE WHEN score.type = "offer_subcategory" THEN score.consultation_id END) AS offer_subcategory_discovery_score
FROM int_metric_prod.discovery_score score
LEFT JOIN int_firebase_prod.consultation consult on score.consultation_id = consult.consultation_id
WHERE unique_session_id IS NOT NULL
{% if is_incremental() %}
AND consultation_date >= date_sub('{{ ds() }}', INTERVAL 3 day)
{% endif %}
GROUP BY consult.unique_session_id,
    consult.user_id,
    consult.consultation_date,
