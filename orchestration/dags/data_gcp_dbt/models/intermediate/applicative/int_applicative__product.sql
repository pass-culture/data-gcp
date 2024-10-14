select
    cast(id as string) as id,
    name,
    description,
    thumbcount,
    jsondata as product_extra_data,
    subcategoryid,
    last_30_days_booking,
    lastproviderid,
    datemodifiedatlastprovider,
    idatproviders,
    ean,
    -- gcucompatibilitytype,
    case when product.thumbcount > 0 then 1 else 0 end as is_mediation
from {{ source("raw", "applicative_database_product") }} product
where product.thumbcount > 0
