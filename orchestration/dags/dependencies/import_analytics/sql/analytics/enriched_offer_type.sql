WITH enriched_items AS (


SELECT 
    offer.item_id, 
    offer.offer_subcategoryId AS subcategory_id,
    subcategories.category_id AS category,
    subcategories.search_group_name AS search_group_name,


    FROM `{{ bigquery_analytics_dataset }}`.enriched_offer_data offer
    JOIN `{{ bigquery_clean_dataset }}`.subcategories subcategories ON offer.offer_subcategoryId = subcategories.id

)


        
        offer.movie_type AS movie_type,
        offer.type AS offer_type_id,
        type_ref.label as offer_type_label,
        offer.subType AS offer_sub_type_id,
        sub_type_ref.sub_label as offer_sub_type_label,
        rayon_ref.macro_rayon as macro_rayon,



LEFT JOIN (SELECT DISTINCT type, label FROM `{{ bigquery_analytics_dataset }}`.offer_types) type_ref
            ON offer.type = cast(type_ref.type as string)
        LEFT JOIN (SELECT DISTINCT sub_type, sub_label FROM `{{ bigquery_analytics_dataset }}`.offer_types) sub_type_ref
            ON offer.subType = cast(sub_type_ref.sub_type as string)
        LEFT JOIN `{{ bigquery_analytics_dataset }}`.macro_rayons AS rayon_ref
            ON offer.rayon = rayon_ref.rayon

