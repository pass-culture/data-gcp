SELECT
    cod.collective_offer_id,
    cod.educational_domain_id,
    ed.educational_domain_name
FROM {{ source('raw', 'applicative_database_collective_offer_domain') }} AS cod
    LEFT JOIN {{ source('raw', 'applicative_database_educational_domain') }} AS ed ON cod.educational_domain_id = ed.educational_domain_id
UNION ALL
SELECT
    cotd.collective_offer_template_id AS collective_offer_id,
    cotd.educational_domain_id,
    ed.educational_domain_name
FROM {{ source('raw', 'applicative_database_collective_offer_template_domain') }} AS cotd
    LEFT JOIN {{ source('raw', 'applicative_database_educational_domain') }} AS ed ON cotd.educational_domain_id = ed.educational_domain_id