{{ config(
    pre_hook="{{ create_humanize_id_function() }}"
) }}
{% set target_name = var('ENV_SHORT_NAME') %}
{% set target_schema = generate_schema_name('analytics_' ~ target_name) %}
with offer_humanized_id as (
    select
        offer_id,
        {{ target_schema }}.humanize_id(offer_id) as humanized_id
    from
        {{ ref('offer') }}
    where
        offer_id is not NULL
),

mediation as (
    select
        offer_id,
        {{ target_schema }}.humanize_id(id) as mediation_humanized_id
    from
        (
            select
                id,
                offerid as offer_id,
                ROW_NUMBER() over (
                    partition by offerid
                    order by
                        datemodifiedatlastprovider desc
                ) as rnk
            from
                {{ source('raw', 'applicative_database_mediation') }}
            where
                isactive
        ) inn
    where
        rnk = 1
),

enriched_items as (

    select
        offer.offer_id,
        offer.offer_creation_date,
        offer.offer_subcategoryid as subcategory_id,
        subcategories.category_id as category_id,
        subcategories.search_group_name as search_group_name,
        case
            when subcategories.category_id = 'MUSIQUE_LIVE' then "MUSIC"
            when subcategories.category_id = 'MUSIQUE_ENREGISTREE' then "MUSIC"
            when subcategories.category_id = 'SPECTACLE' then "SHOW"
            when subcategories.category_id = 'CINEMA' then "MOVIE"
            when subcategories.category_id = 'LIVRE' then "BOOK"
        end as offer_type_domain,
        case
            when (
                offer.offer_name is NULL
                or offer.offer_name = 'NaN'
            ) then "None"
            else SAFE_CAST(offer.offer_name as STRING)
        end as offer_name,
        case
            when (
                offer.offer_description is NULL
                or offer.offer_description = 'NaN'
            ) then "None"
            else SAFE_CAST(offer.offer_description as STRING)
        end as offer_description,
        case
            when mediation.mediation_humanized_id is not NULL
                then CONCAT(
                        'https://storage.googleapis.com/',
                        {{ get_mediation_url() }} || '-assets-fine-grained/thumbs/mediations/',
                        mediation.mediation_humanized_id
                    )
            else CONCAT(
                    'https://storage.googleapis.com/',
                    {{ get_mediation_url() }} || '-assets-fine-grained/thumbs/products/',
                    {{ target_schema }}.humanize_id(offer.offer_product_id)
                )
        end as image_url
    from {{ ref('offer') }} offer
        join {{ source('raw','subcategories') }} subcategories on offer.offer_subcategoryid = subcategories.id
        left join mediation on offer.offer_id = mediation.offer_id
),

offer_types as (
    select distinct
        UPPER(domain) as offer_type_domain,
        CAST(type as STRING) as offer_type_id,
        label as offer_type_label
    from {{ source('analytics','offer_types') }} offer
),

offer_sub_types as (
    select distinct
        UPPER(domain) as offer_type_domain,
        CAST(type as STRING) as offer_type_id,
        label as offer_type_label,
        SAFE_CAST(SAFE_CAST(sub_type as FLOAT64) as STRING) as offer_sub_type_id,
        sub_label as offer_sub_type_label
    from {{ source('analytics','offer_types') }} offer
),

offer_metadata_id as (
    select
        enriched_items.*,
        case
            when enriched_items.offer_type_domain = "MUSIC" and int_applicative__extract_offer.musictype != '' then int_applicative__extract_offer.musictype
            when enriched_items.offer_type_domain = "SHOW" and int_applicative__extract_offer.showtype != '' then int_applicative__extract_offer.showtype
        end as offer_type_id,
        case
            when enriched_items.offer_type_domain = "MUSIC" and int_applicative__extract_offer.musictype != '' then int_applicative__extract_offer.musicsubtype
            when enriched_items.offer_type_domain = "SHOW" and int_applicative__extract_offer.showtype != '' then int_applicative__extract_offer.showsubtype
        end as offer_sub_type_id,
        int_applicative__extract_offer.rayon,
        int_applicative__extract_offer.genres,
        int_applicative__extract_offer.author,
        int_applicative__extract_offer.performer,
        int_applicative__extract_offer.titelive_gtl_id as titelive_gtl_id,
        gtl.gtl_type as gtl_type,
        gtl.gtl_label_level_1,
        gtl.gtl_label_level_2,
        gtl.gtl_label_level_3,
        gtl.gtl_label_level_4
    from enriched_items
        left join {{ ref('int_applicative__extract_offer') }} as int_applicative__extract_offer on int_applicative__extract_offer.offer_id = enriched_items.offer_id
        left join {{ ref('int_applicative__titelive_gtl') }} gtl on int_applicative__extract_offer.titelive_gtl_id = gtl.gtl_id and gtl.gtl_type = enriched_items.offer_type_domain

),


offer_metadata as (
    select
        omi.* except (genres, rayon),
        case
            when omi.offer_type_domain = "MUSIC" then offer_types.offer_type_label
            when omi.offer_type_domain = "SHOW" then offer_types.offer_type_label
            when omi.offer_type_domain = "MOVIE" then REGEXP_EXTRACT_ALL(UPPER(genres), r'[0-9a-zA-Z][^"]+')[SAFE_OFFSET(0)] -- array of string, take first only
            when omi.offer_type_domain = "BOOK" then macro_rayons.macro_rayon
        end as offer_type_label,

        case
            when omi.offer_type_domain = "MUSIC" then IF(offer_types.offer_type_label is NULL, NULL, [offer_types.offer_type_label])
            when omi.offer_type_domain = "SHOW" then IF(offer_types.offer_type_label is NULL, NULL, [offer_types.offer_type_label])
            when omi.offer_type_domain = "MOVIE" then REGEXP_EXTRACT_ALL(UPPER(genres), r'[0-9a-zA-Z][^"]+') -- array of string convert to list
            when omi.offer_type_domain = "BOOK" then IF(macro_rayons.macro_rayon is NULL, NULL, [macro_rayons.macro_rayon])
        end as offer_type_labels,

        case
            when omi.offer_type_domain = "MUSIC" then offer_sub_types.offer_sub_type_label
            when omi.offer_type_domain = "SHOW" then offer_sub_types.offer_sub_type_label
            when omi.offer_type_domain = "MOVIE" then NULL -- no sub-genre here
            when omi.offer_type_domain = "BOOK" then omi.rayon
        end as offer_sub_type_label

    from offer_metadata_id omi

        left join offer_types
            on offer_types.offer_type_domain = omi.offer_type_domain
                and offer_types.offer_type_id = omi.offer_type_id

        left join offer_sub_types
            on offer_sub_types.offer_type_domain = omi.offer_type_domain
                and offer_sub_types.offer_type_id = omi.offer_type_id
                and offer_sub_types.offer_sub_type_id = omi.offer_sub_type_id
        left join {{ source('seed','macro_rayons') }} on omi.rayon = macro_rayons.rayon
)

select *
from offer_metadata
