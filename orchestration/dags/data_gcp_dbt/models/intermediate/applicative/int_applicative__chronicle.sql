SELECT
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
    DATE(c.chronicle_creation_date) AS chronicle_creation_date

FROM {{ ref("raw_applicative__chronicle") }} AS c
LEFT JOIN {{ ref("raw_applicative__offer_chronicle") }} AS oc ON c.chronicle_id = oc.chronicle_id
LEFT JOIN {{ ref("raw_applicative__product_chronicle") }} AS pc ON c.chronicle_id = pc.chronicle_id
