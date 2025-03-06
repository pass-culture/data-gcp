select
    c.chronicle_id,
    oc.offer_id,
    pc.product_id,
    c.user_id,
    c.user_is_active,
    c.user_is_identity_diffusible,
    c.user_is_social_media_diffusible,
    c.user_city,
    c.user_age,
    c.ean,
    c.chronicle_content,
    date(c.chronicle_creation_date) as chronicle_creation_date

from {{ ref("raw_applicative__chronicle") }} as c
left join
    {{ ref("raw_applicative__offer_chronicle") }} as oc
    on c.chronicle_id = oc.chronicle_id
left join
    {{ ref("raw_applicative__product_chronicle") }} as pc
    on c.chronicle_id = pc.chronicle_id
