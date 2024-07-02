SELECT
    vc.venue_id,
    v.venue_managing_offerer_id,
    v.offerer_rank_desc,
    vc.criterion_id AS venue_tag_id,
    ccm.criterion_category_id AS venue_tag_category_id,
    cc.criterion_category_label AS venue_tag_category_label,
    c.name AS venue_tag_name,
FROM {{ source('raw', 'applicative_database_venue_criterion') }} AS vc
LEFT JOIN {{ ref("mrt_global__venue") }} AS v ON vc.venue_id = v.venue_id
INNER JOIN {{ source('raw', 'applicative_database_criterion_category_mapping') }} AS ccm
    ON ccm.criterion_id = vc.criterion_id
INNER JOIN {{ source('raw', 'applicative_database_criterion_category') }} AS cc
    ON cc.criterion_category_id = ccm.criterion_category_id
INNER JOIN {{ source('raw', 'applicative_database_criterion') }} AS c ON ccm.criterion_id = c.id
