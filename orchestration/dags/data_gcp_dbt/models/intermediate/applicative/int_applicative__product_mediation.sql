select
    cast(id as string) as id,
    datemodifiedatlastprovider as date_modified_at_last_provider,
    cast(productid as string) as product_id,
    imagetype as image_type,
    cast(lastproviderid as string) as last_provider_id,
    uuid as uuid
from {{ source("raw", "applicative_database_product_mediation") }} product_mediation
