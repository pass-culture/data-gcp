{{ config(
    pre_hook="{{ create_humanize_id_function() }}"
) }}
{% set target_name = target.name %}
{% set target_schema = generate_schema_name('analytics_dbt_' ~ target_name) %}
WITH offer_humanized_id AS (
    SELECT
        offer_id,
        {{target_schema}}.humanize_id(offer_id) AS humanized_id,
    FROM
        {{ ref('offer') }}
    WHERE
        offer_id is not NULL
),
mediation AS (
    SELECT
        offer_id,
        {{target_schema}}.humanize_id(id) as mediation_humanized_id
    FROM
        (
            SELECT
                id,
                offerId as offer_id,
                ROW_NUMBER() OVER (
                    PARTITION BY offerId
                    ORDER BY
                        dateModifiedAtLastProvider DESC
                ) as rnk
            FROM
                {{ source('raw', 'applicative_database_mediation') }} 
            WHERE
                isActive
        ) inn
    WHERE
        rnk = 1
),
enriched_items AS (

    SELECT 
        offer.offer_id,
        offer.offer_subcategoryId AS subcategory_id,
        subcategories.category_id AS category_id,
        subcategories.search_group_name AS search_group_name,
        CASE
            WHEN subcategories.category_id = 'MUSIQUE_LIVE' THEN "MUSIC"
            WHEN subcategories.category_id = 'MUSIQUE_ENREGISTREE'  THEN "MUSIC" 
            WHEN subcategories.category_id = 'SPECTACLE' THEN "SHOW"
            WHEN subcategories.category_id = 'CINEMA' THEN "MOVIE"
            WHEN subcategories.category_id = 'LIVRE' THEN "BOOK"
        END AS offer_type_domain,
        CASE
            when (
                offer.offer_name is null
                or offer.offer_name = 'NaN'
            ) then "None"
            else safe_cast(offer.offer_name as STRING)
        END as offer_name,
        CASE
            when (
                offer.offer_description is null
                or offer.offer_description = 'NaN'
            ) then "None"
            else safe_cast(offer.offer_description as STRING)
        END as offer_description,
        CASE
            WHEN mediation.mediation_humanized_id is not null THEN CONCAT(
                "https://storage.googleapis.com/{{ mediation_url }}-assets-fine-grained/thumbs/mediations/",
                mediation.mediation_humanized_id
            )
            ELSE CONCAT(
                "https://storage.googleapis.com/{{ mediation_url }}-assets-fine-grained/thumbs/products/",
                {{target_schema}}.humanize_id(offer.offer_product_id)
            )
        END AS image_url

    FROM {{ ref('offer') }} offer
    JOIN {{ source('clean','subcategories') }} subcategories ON offer.offer_subcategoryId = subcategories.id
    LEFT JOIN mediation ON offer.offer_id = mediation.offer_id
),

offer_types AS (
    SELECT
      DISTINCT
        upper(domain) as offer_type_domain, 
         CAST(type AS STRING) as offer_type_id,
        label as offer_type_label
    FROM {{ source('analytics','offer_types') }} offer
),

offer_sub_types AS (
    SELECT
      DISTINCT
        upper(domain) as offer_type_domain, 
         CAST(type AS STRING) as offer_type_id,
        label as offer_type_label,
        SAFE_CAST(SAFE_CAST(sub_type AS FLOAT64) AS STRING) as offer_sub_type_id,
        sub_label as offer_sub_type_label,
    FROM {{ source('analytics','offer_types') }} offer
),

offer_metadata_id AS (
    SELECT
        enriched_items.*, 
        CASE
            WHEN enriched_items.offer_type_domain = "MUSIC" AND offer_extracted_data.musicType != '' THEN  offer_extracted_data.musicType
            WHEN enriched_items.offer_type_domain = "SHOW" AND offer_extracted_data.showType != '' THEN  offer_extracted_data.showType
        END AS offer_type_id,
        CASE
            WHEN enriched_items.offer_type_domain = "MUSIC" AND offer_extracted_data.musicType != '' THEN  offer_extracted_data.musicSubtype
            WHEN enriched_items.offer_type_domain = "SHOW" AND offer_extracted_data.showType != '' THEN  offer_extracted_data.showSubType
        END AS offer_sub_type_id,
        offer_extracted_data.rayon,
        offer_extracted_data.genres,
        offer_extracted_data.author,
        offer_extracted_data.performer,
        gtl.gtl_type,
        gtl.gtl_id titelive_gtl_id,
        gtl.gtl_label_level_1,
        gtl.gtl_label_level_2,
        gtl.gtl_label_level_3,
        gtl.gtl_label_level_4
    FROM enriched_items 
    LEFT JOIN {{ ref('offer_extracted_data') }} as offer_extracted_data ON offer_extracted_data.offer_id = enriched_items.offer_id
    LEFT JOIN {{ ref('int_applicative__titelive_gtl') }} gtl ON offer_extracted_data.titelive_gtl_id = gtl.gtl_id and gtl.gtl_type = enriched_items.offer_type_domain
    
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
    LEFT JOIN {{ source('raw','macro_rayons') }} ON omi.rayon = macro_rayons.rayon
)

SELECT 
*
FROM offer_metadata