select *
from {{ source('raw', 'applicative_database_validation_rule_offer_link') }}
