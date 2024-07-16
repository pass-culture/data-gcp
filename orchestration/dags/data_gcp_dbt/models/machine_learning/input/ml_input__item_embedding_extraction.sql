{{
    config(
        materialized = "view"
    )
}}

WITH last_extraction AS (
    SELECT distinct item_id
    FROM {{ source('ml_preproc', 'item_embedding_extraction') }}
    WHERE date(extraction_date) > DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
) 

SELECT 
    im.item_id,
    im.subcategory_id,
    im.category_id,
    im.offer_name,
    im.offer_description,
    im.image_url as image,
    CASE 
        WHEN titelive_gtl_id is not NULL 
        THEN  TRIM(
        CONCAT(
            COALESCE(gtl_label_level_1, ''),
            ' ',
            COALESCE(gtl_label_level_2, ''),
            ' ',
            COALESCE(gtl_label_level_3, ''),
            ' ',
            COALESCE(gtl_label_level_4, '')
        )
        )
        WHEN offer_type_label is not null THEN TRIM(ARRAY_TO_STRING(offer_type_labels, ' ')) 
    END AS offer_label_concat,
    TRIM(
    CONCAT(
        COALESCE(author, ''),
        ' ',
        COALESCE(performer, '')
    )
    ) as author_concat,
    offer_creation_date
        
  FROM {{ ref('item_metadata')}} im
  LEFT JOIN last_extraction le ON le.item_id = im.item_id
  WHERE le.item_id IS NULL

ORDER BY offer_creation_date DESC
