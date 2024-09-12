with offer_types as (
    select distinct
        UPPER(domain) as offer_type_domain,
        CAST(type as STRING) as offer_type_id,
        label as offer_type_label
    from {{ source('raw','offer_types') }}
),

offer_sub_types as (
    select distinct
        UPPER(domain) as offer_type_domain,
        CAST(type as STRING) as offer_type_id,
        label as offer_type_label,
        SAFE_CAST(SAFE_CAST(sub_type as FLOAT64) as STRING) as offer_sub_type_id,
        sub_label as offer_sub_type_label
    from {{ source('raw','offer_types') }}
)


select
    o.offer_id,
    o.offer_creation_date,
    o.offer_subcategory_id,
    o.offer_category_id,
    o.search_group_name,
    o.offer_type_domain,
    o.offer_name,
    o.offer_description,
    o.author,
    o.performer,
    o.titelive_gtl_id,
    o.offer_type_id,
    o.offer_sub_type_id,
    case
        when o.mediation_humanized_id is not NULL
            then CONCAT(
                    'https://storage.googleapis.com/',
                    {{ get_mediation_url() }} || '-assets-fine-grained/thumbs/mediations/',
                    mediation_humanized_id
                )
        else CONCAT(
                'https://storage.googleapis.com/',
                {{ get_mediation_url() }} || '-assets-fine-grained/thumbs/products/',
                offer_product_humanized_id
            )
        end as image_url,
    gtl.gtl_type,
    gtl.gtl_label_level_1,
    gtl.gtl_label_level_2,
    gtl.gtl_label_level_3,
    gtl.gtl_label_level_4,
    case
        when o.offer_type_domain = "MUSIC" then offer_types.offer_type_label
        when o.offer_type_domain = "SHOW" then offer_types.offer_type_label
        when o.offer_type_domain = "MOVIE" then REGEXP_EXTRACT_ALL(UPPER(genres), r'[0-9a-zA-Z][^"]+')[SAFE_OFFSET(0)]
        when o.offer_type_domain = "BOOK" then macro_rayons.macro_rayon
    end as offer_type_label,

    case
        when o.offer_type_domain = "MUSIC" then IF(offer_types.offer_type_label is NULL, NULL, [offer_types.offer_type_label])
        when o.offer_type_domain = "SHOW" then IF(offer_types.offer_type_label is NULL, NULL, [offer_types.offer_type_label])
        when o.offer_type_domain = "MOVIE" then REGEXP_EXTRACT_ALL(UPPER(genres), r'[0-9a-zA-Z][^"]+')
        when o.offer_type_domain = "BOOK" then IF(macro_rayons.macro_rayon is NULL, NULL, [macro_rayons.macro_rayon])
    end as offer_type_labels,

    case
        when o.offer_type_domain = "MUSIC" then offer_sub_types.offer_sub_type_label
        when o.offer_type_domain = "SHOW" then offer_sub_types.offer_sub_type_label
        when o.offer_type_domain = "MOVIE" then NULL
        when o.offer_type_domain = "BOOK" then o.rayon
    end as offer_sub_type_label

from {{ ref('int_applicative__offer') }} AS o

    left join {{ ref('int_applicative__titelive_gtl') }} AS gtl on o.titelive_gtl_id = gtl.gtl_id and gtl.gtl_type = o.offer_type_domain

    left join offer_types
        on offer_types.offer_type_domain = o.offer_type_domain
            and offer_types.offer_type_id = o.offer_type_id

    left join offer_sub_types
        on offer_sub_types.offer_type_domain = o.offer_type_domain
            and offer_sub_types.offer_type_id = o.offer_type_id
            and offer_sub_types.offer_sub_type_id = o.offer_sub_type_id
    left join {{ source('seed','macro_rayons') }} on o.rayon = macro_rayons.rayon
