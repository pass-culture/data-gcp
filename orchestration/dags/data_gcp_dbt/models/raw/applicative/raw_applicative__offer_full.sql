select
    * except (offer_updated_date, offer_fields_updated),
    timestamp(offer_updated_date) as offer_updated_date,
    to_hex(md5(to_json_string(offer))) as custom_scd_id
from {{ source("raw", "applicative_database_offer_legacy") }} as offer
