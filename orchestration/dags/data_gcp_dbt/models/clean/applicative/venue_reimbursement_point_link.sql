select *
from {{ source('raw', 'applicative_database_venue_reimbursement_point_link') }}
