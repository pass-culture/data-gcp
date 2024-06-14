SELECT
    venue_id,
    venue_tag_id,
    venue_tag_category_id,
    venue_tag_category_label,
    venue_tag_name,
FROM {{ ref('int_applicative__venue_criterion') }}
