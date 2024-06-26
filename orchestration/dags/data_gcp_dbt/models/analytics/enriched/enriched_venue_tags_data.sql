SELECT
    venue_criterion.venue_id
    , global_venue.partner_id
    , venue_criterion.criterion_id
    , criterion_category_mapping.criterion_category_id
    , criterion_category.criterion_category_label
    , criterion.name criterion_name
FROM {{ source('raw', 'applicative_database_venue_criterion') }} AS venue_criterion
JOIN {{ source('raw', 'applicative_database_criterion_category_mapping') }} AS criterion_category_mapping
    USING(criterion_id)
JOIN {{ source('raw', 'applicative_database_criterion_category') }} AS criterion_category
    USING(criterion_category_id)
JOIN {{ source('raw', 'applicative_database_criterion') }} AS criterion ON criterion_category_mapping.criterion_id = criterion.id
LEFT JOIN {{ ref('mrt_global__venue') }} AS global_venue ON global_venue.venue_id=venue_criterion.venue_id