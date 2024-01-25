SELECT 
    cod.collective_offer_id,
    cod.educational_domain_id,
    ed.educational_domain_name
FROM {{ source('raw', 'applicative_database_collective_offer_domain') }} cod
LEFT JOIN {{ source('raw', 'applicative_database_educational_domain') }}ed ON cod.educational_domain_id = ed.educational_domain_id