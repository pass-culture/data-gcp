SELECT
    oa.offerer_address_id,
    oa.offerer_address_label,
    oa.address_id,
    oa.offerer_id,
    a.address_street,
    a.address_postal_code,
    a.address_city,
    a.address_departement_code
FROM {{ source('raw', 'applicative_database_offerer_address') }} oa
LEFT JOIN {{ source('raw', 'applicative_database_address') }} a ON oa.address_id=a.address_id
