-- This model is an example
SELECT fc.consultation_date,
    fc.user_id,
    fc.origin,
    fc.unique_session_id,
    COUNT(DISTINCT CASE WHEN mds.consulted_entity = 'item' THEN mds.consultation_id END) AS item_score,
    COUNT(DISTINCT CASE WHEN mds.consulted_entity = 'offer_subcategory_id' THEN mds.consultation_id END) AS subcategory_score,
    COUNT(DISTINCT CASE WHEN mds.consulted_entity = 'offer_category_id' THEN mds.consultation_id END) AS category_score
FROM {{ ref('int_metric__discovery_score') }} AS mds
LEFT JOIN {{ ref('int_firebase__consultation') }} as fc ON mds.consultation_id = fc.consultation_id
GROUP BY consultation_date,
    user_id,
    origin,
    unique_session_id
