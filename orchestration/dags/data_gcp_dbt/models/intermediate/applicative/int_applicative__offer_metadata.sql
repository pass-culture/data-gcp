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
            concat(
                'https://storage.googleapis.com/',
                {{ get_mediation_url() }} || '-assets-fine-grained/thumbs/mediations/',
                mediation_humanized_id
            )
        else
            concat(
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
    o.offer_type_label,
    o.offer_type_labels,
    o.offer_sub_type_label

from {{ ref("int_applicative__offer") }} as o

left join
    {{ ref("int_applicative__titelive_gtl") }} as gtl
    on o.titelive_gtl_id = gtl.gtl_id
    and gtl.gtl_type = o.offer_type_domain


