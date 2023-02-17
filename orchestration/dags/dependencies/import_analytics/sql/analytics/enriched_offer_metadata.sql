WITH enriched_items AS (

    SELECT 
        offer.offer_id ,
        offer.offer_subcategoryId AS subcategory_id,
        subcategories.category_id AS category_id,
        subcategories.search_group_name AS search_group_name,
        CASE
            WHEN subcategories.category_id = 'MUSIQUE_LIVE' THEN "MUSIC"
            WHEN subcategories.category_id = 'MUSIQUE_ENREGISTREE'  THEN "MUSIC" 
            WHEN subcategories.category_id = 'SPECTACLE' THEN "SHOW"
            WHEN subcategories.category_id = 'CINEMA' THEN "MOVIE"
            WHEN subcategories.category_id = 'LIVRE' THEN "BOOK"
        END AS offer_type_domain
    FROM `{{ bigquery_clean_dataset }}`.applicative_database_offer offer
    JOIN `{{ bigquery_clean_dataset }}`.subcategories subcategories ON offer.offer_subcategoryId = subcategories.id
),

offer_types AS (
    SELECT
      DISTINCT
        upper(domain) as offer_type_domain, 
         CAST(type AS STRING) as offer_type_id,
        label as offer_type_label
    FROM `{{ bigquery_analytics_dataset }}`.offer_types offer
),

offer_sub_types AS (
    SELECT
      DISTINCT
        upper(domain) as offer_type_domain, 
         CAST(type AS STRING) as offer_type_id,
        label as offer_type_label,
        SAFE_CAST(SAFE_CAST(sub_type AS FLOAT64) AS STRING) as offer_sub_type_id,
        sub_label as offer_sub_type_label,
    FROM `{{ bigquery_analytics_dataset }}`.offer_types offer
),

offer_metadata_id AS (
    SELECT
        enriched_items.*, 
        CASE
            WHEN offer_type_domain = "MUSIC" AND offer_extracted_data.musicType != '' THEN  offer_extracted_data.musicType
            WHEN offer_type_domain = "SHOW" AND offer_extracted_data.showType != '' THEN  offer_extracted_data.showType
        END AS offer_type_id,
        CASE
            WHEN offer_type_domain = "MUSIC" AND offer_extracted_data.musicType != '' THEN  offer_extracted_data.musicSubtype
            WHEN offer_type_domain = "SHOW" AND offer_extracted_data.showType != '' THEN  offer_extracted_data.showSubType
        END AS offer_sub_type_id,

        offer_extracted_data.rayon,
        offer_extracted_data.genres


    FROM enriched_items
    
    LEFT JOIN `{{ bigquery_clean_dataset }}`.offer_extracted_data offer_extracted_data ON offer_extracted_data.offer_id = enriched_items.offer_id
),


offer_metadata AS (
    SELECT
        omi.* except(genres, rayon), 
        CASE
            WHEN omi.offer_type_domain = "MUSIC" THEN offer_types.offer_type_label
            WHEN omi.offer_type_domain = "SHOW"  THEN offer_types.offer_type_label 
            WHEN omi.offer_type_domain = "MOVIE" THEN REGEXP_EXTRACT_ALL(UPPER(genres), r'[0-9a-zA-Z][^"]+')[safe_offset(0)] -- array of string, take first only
            WHEN omi.offer_type_domain = "BOOK"  THEN macro_rayons.macro_rayon
        END AS offer_type_label,

        CASE
            WHEN omi.offer_type_domain = "MUSIC" THEN if(offer_types.offer_type_label is null, NULL, [offer_types.offer_type_label])
            WHEN omi.offer_type_domain = "SHOW"  THEN if(offer_types.offer_type_label is null, NULL, [offer_types.offer_type_label])
            WHEN omi.offer_type_domain = "MOVIE" THEN REGEXP_EXTRACT_ALL(UPPER(genres), r'[0-9a-zA-Z][^"]+') -- array of string convert to list
            WHEN omi.offer_type_domain = "BOOK"  THEN if(macro_rayons.macro_rayon is null, NULL, [macro_rayons.macro_rayon])
        END AS offer_type_labels,

        CASE
            WHEN omi.offer_type_domain = "MUSIC" THEN offer_sub_types.offer_sub_type_label
            WHEN omi.offer_type_domain = "SHOW"  THEN offer_sub_types.offer_sub_type_label
            WHEN omi.offer_type_domain = "MOVIE" THEN NULL -- no sub-genre here
            WHEN omi.offer_type_domain = "BOOK"  THEN omi.rayon
        END AS offer_sub_type_label
    FROM offer_metadata_id omi

    LEFT JOIN offer_types 
        ON offer_types.offer_type_domain = omi.offer_type_domain 
        AND  offer_types.offer_type_id = omi.offer_type_id

    LEFT JOIN offer_sub_types 
        ON offer_sub_types.offer_type_domain = omi.offer_type_domain 
        AND  offer_sub_types.offer_type_id = omi.offer_type_id
        AND  offer_sub_types.offer_sub_type_id = omi.offer_sub_type_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.macro_rayons ON omi.rayon = macro_rayons.rayon
)

SELECT 
*
FROM offer_metadata