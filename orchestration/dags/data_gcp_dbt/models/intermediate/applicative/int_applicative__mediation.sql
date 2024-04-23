SELECT
    thumbCount,
    idAtProviders,
    dateModifiedAtLastProvider,
    id,
    dateCreated,
    authorId,
    lastProviderId,
    offerId AS offer_id,
    credit,
    isActive,
    fieldsUpdated,
    CASE WHEN isActive AND thumbCount > 0 THEN 1 ELSE 0 END AS is_mediation
FROM {{ source('raw', 'applicative_database_mediation') }}
