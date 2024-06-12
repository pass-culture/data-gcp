SELECT p.is_active,
    p.provider_id,
    p.provider_name,
    p.local_class,
    p.enabled_for_pro,
    vp.venue_id,
    vp.last_sync_date,
    vp.creation_date,
FROM {{ source('raw','applicative_database_provider') }} AS p
INNER JOIN {{ source('raw','applicative_database_venue_provider') }} AS vp ON vp.provider_id = p.provider_id
