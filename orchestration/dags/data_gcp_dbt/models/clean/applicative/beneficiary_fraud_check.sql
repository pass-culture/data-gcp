select *
from {{ source('raw', 'applicative_database_beneficiary_fraud_check') }}
