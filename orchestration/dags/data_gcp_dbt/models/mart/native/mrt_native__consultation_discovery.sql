{{
    config(
        **custom_incremental_config(
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'consultation_date', 'data_type': 'date'}
    )
) }}

SELECT 
    consult.user_id,
    consult.consultation_date,
    score.consultation_id,
    consult.origin,
    consult.offer_id,
    consult.unique_session_id,
    offer.offer_name,
    offer.offer_category_id,
    offer.offer_subcategory_id,
    offer.venue_id,
    offer.venue_name,
    offer.venue_type_label,
    offer.partner_id,
    offer.offerer_id,
    user.user_region_name,
    user.user_department_code,
    user.user_activity,
    user.user_is_priority_public,
    user.user_is_unemployed,
    user.user_is_in_qpv,
    user.user_macro_density_label,
    ANY_VALUE(CASE WHEN score.type = "item" THEN score.consulted_entity END) AS item_id,
    ANY_VALUE(CASE WHEN score.type = "offer_category" THEN score.consulted_entity END) AS discovered_category_id,
    ANY_VALUE(CASE WHEN score.type = "offer_subcategory" THEN score.consulted_entity END) AS discovered_subcategory_id,
    COUNT(score.consultation_id) AS consultation_discovery_score,
    SUM(CASE WHEN score.type = "item" THEN 1 ELSE 0 END) AS item_discovery_score,
    SUM(CASE WHEN score.type = "offer_category" THEN 1 ELSE 0 END) AS category_discovery_score,
    SUM(CASE WHEN score.type = "offer_subcategory" THEN 1 ELSE 0 END) AS subcategory_discovery_score,
    CASE WHEN ANY_VALUE(CASE WHEN score.type = "offer_category" THEN score.consulted_entity END) IS NOT NULL THEN TRUE ELSE FALSE END AS is_category_discovered,
    CASE WHEN ANY_VALUE(CASE WHEN score.type = "offer_subcategory" THEN score.consulted_entity END) IS NOT NULL THEN TRUE ELSE FALSE END AS is_subcategory_discovered,

FROM {{ ref('int_metric__discovery_score')}} score 
LEFT JOIN {{ ref('int_firebase__native_consultation')}} consult on score.consultation_id = consult.consultation_id 
LEFT JOIN {{ ref('int_global__offer')}} offer on consult.offer_id = offer.offer_id
LEFT JOIN {{ ref('int_global__user')}} user on consult.user_id = user.user_id
{% if is_incremental() %}
WHERE consultation_date >= date_sub('{{ ds() }}', INTERVAL 3 day)
{% endif %}
GROUP BY consult.user_id,
    consult.consultation_date,
    score.consultation_id,
    consult.origin,
    consult.offer_id,
    consult.unique_session_id,
    offer.offer_name,
    offer.offer_category_id,
    offer.offer_subcategory_id,
    offer.venue_id,
    offer.venue_name,
    offer.venue_type_label,
    offer.partner_id,
    offer.offerer_id,
    user.user_region_name,
    user.user_department_code,
    user.user_activity,
    user.user_is_priority_public,
    user.user_is_unemployed,
    user.user_is_in_qpv,
    user.user_macro_density_label,