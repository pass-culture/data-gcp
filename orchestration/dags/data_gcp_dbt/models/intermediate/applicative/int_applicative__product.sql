SELECT CAST(id AS string) AS id,
    name,
    description,
    thumbCount,
    jsonData as product_extra_data,
    subcategoryId,
    last_30_days_booking,
    lastProviderId,
    dateModifiedAtLastProvider,
    idAtProviders,
    ean,
    --gcucompatibilitytype,
    CASE WHEN product.thumbCount > 0 THEN 1 ELSE 0 END AS is_mediation
FROM {{ source('raw', 'applicative_database_product') }} product
where product.thumbCount > 0
