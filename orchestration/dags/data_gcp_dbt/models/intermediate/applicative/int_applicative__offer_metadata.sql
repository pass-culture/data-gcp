with
    offer_types as (
        select distinct
            cast(type as string) as offer_type_id,
            label as offer_type_label,
            upper(domain) as offer_type_domain
        from {{ source("raw", "offer_types") }}
    ),

    offer_sub_types as (
        select distinct
            cast(type as string) as offer_type_id,
            label as offer_type_label,
            sub_label as offer_sub_type_label,
            upper(domain) as offer_type_domain,
            safe_cast(safe_cast(sub_type as float64) as string) as offer_sub_type_id
        from {{ source("raw", "offer_types") }}
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
        when o.mediation_humanized_id is not null
        then
            case
                when m.thumb_count > 1
                then
                    concat(
                        'https://storage.googleapis.com/',
                        {{ get_mediation_url() }}
                        || '-assets-fine-grained/thumbs/mediations/',
                        o.mediation_humanized_id,
                        '_',
                        m.thumb_count - 1
                    )
                when m.thumb_count = 1
                then
                    concat(
                        'https://storage.googleapis.com/',
                        {{ get_mediation_url() }}
                        || '-assets-fine-grained/thumbs/mediations/',
                        o.mediation_humanized_id
                    )
            end
        when o.offer_product_id is not null
        then
            case
                when pm.uuid is not null
                then
                    concat(
                        'https://storage.googleapis.com/',
                        {{ get_mediation_url() }} || '-assets-fine-grained/thumbs/',
                        pm.uuid
                    )
                else
                    case
                        when p.thumbcount > 1
                        then
                            concat(
                                'https://storage.googleapis.com/',
                                {{ get_mediation_url() }}
                                || '-assets-fine-grained/thumbs/products/',
                                o.offer_product_humanized_id,
                                '_',
                                p.thumbcount - 1
                            )
                        when p.thumbcount = 1
                        then
                            concat(
                                'https://storage.googleapis.com/',
                                {{ get_mediation_url() }}
                                || '-assets-fine-grained/thumbs/products/',
                                o.offer_product_humanized_id
                            )
                    end
            end

    end as image_url,
    omd.videourl as offer_video_url,
    gtl.gtl_type,
    gtl.gtl_label_level_1,
    gtl.gtl_label_level_2,
    gtl.gtl_label_level_3,
    gtl.gtl_label_level_4,
    case
        when o.offer_type_domain = 'MUSIC'
        then offer_types.offer_type_label
        when o.offer_type_domain = 'SHOW'
        then offer_types.offer_type_label
        when o.offer_type_domain = 'MOVIE'
        then regexp_extract_all(upper(genres), r'[0-9a-zA-Z][^"]+')[safe_offset(0)]
        when o.offer_type_domain = 'BOOK'
        then macro_rayons.macro_rayon
    end as offer_type_label,

    case
        when o.offer_type_domain = 'MUSIC'
        then
            if(
                offer_types.offer_type_label is null,
                null,
                [offer_types.offer_type_label]
            )
        when o.offer_type_domain = 'SHOW'
        then
            if(
                offer_types.offer_type_label is null,
                null,
                [offer_types.offer_type_label]
            )
        when o.offer_type_domain = 'MOVIE'
        then regexp_extract_all(upper(genres), r'[0-9a-zA-Z][^"]+')
        when o.offer_type_domain = 'BOOK'
        then if(macro_rayons.macro_rayon is null, null, [macro_rayons.macro_rayon])
    end as offer_type_labels,

    case
        when o.offer_type_domain = 'MUSIC'
        then offer_sub_types.offer_sub_type_label
        when o.offer_type_domain = 'SHOW'
        then offer_sub_types.offer_sub_type_label
        when o.offer_type_domain = 'MOVIE'
        then null
        when o.offer_type_domain = 'BOOK'
        then o.rayon
    end as offer_sub_type_label

from {{ ref("int_applicative__offer") }} as o

left join
    {{ ref("int_applicative__titelive_gtl") }} as gtl
    on o.titelive_gtl_id = gtl.gtl_id
    and o.offer_type_domain = gtl.gtl_type

left join
    offer_types
    on o.offer_type_domain = offer_types.offer_type_domain
    and o.offer_type_id = offer_types.offer_type_id

left join
    offer_sub_types
    on o.offer_type_domain = offer_sub_types.offer_type_domain
    and o.offer_type_id = offer_sub_types.offer_type_id
    and o.offer_sub_type_id = offer_sub_types.offer_sub_type_id
left join {{ source("seed", "macro_rayons") }} on o.rayon = macro_rayons.rayon
left join {{ ref("int_applicative__mediation") }} as m on o.offer_id = m.offer_id
left join
    {{ ref("int_applicative__product_mediation") }} as pm
    on o.offer_product_id = pm.product_id
left join {{ ref("int_applicative__product") }} as p on o.offer_product_id = p.id
left join {{ source("raw", "applicative_database_offer_meta_data") }} omd on omd.offerid=o.offer_id
qualify row_number() over (partition by offer_id order by pm.image_type) = 1
