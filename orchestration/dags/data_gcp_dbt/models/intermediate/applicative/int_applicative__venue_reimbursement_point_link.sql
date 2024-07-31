select
    vpl.id venue_reimbursement_point_link_id,
    vpl.venue_id,
    vpl.reimbursement_point_id,
    vpl.reimbursement_point_link_beginning_date,
    vpl.reimbursement_point_link_ending_date,
    v.venue_managing_offerer_id as offerer_id
from {{ source('raw', 'applicative_database_venue') }} as v
    left join {{ source('raw', 'applicative_database_venue_reimbursement_point_link') }} as vpl on v.venue_id = vpl.venue_id
