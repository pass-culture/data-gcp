SELECT
    r.end_date AS response_date,
    v.venue_id,
    r.response_id,
    v.venue_region_name,
    v.venue_type_label,
    v.venue_is_permanent,
    SAFE_CAST(r.answer AS INT64) AS rating,
    DATE_DIFF(DATE(r.end_date), v.venue_creation_date, DAY) AS venue_seniority
FROM {{ ref('int_qualtrics__nps_venue_answer') }} AS r
INNER JOIN {{ ref('mrt_global__venue') }} AS v ON r.venue_id = v.venue_id

WHERE r.is_nps_question = true
