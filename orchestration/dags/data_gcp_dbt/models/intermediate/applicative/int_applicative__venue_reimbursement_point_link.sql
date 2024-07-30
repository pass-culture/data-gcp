SELECT vpl.id venue_reimbursement_point_link_id,
    vpl.venue_id,
    vpl.reimbursement_point_id,
    vpl.reimbursement_point_link_beginning_date,
    vpl.reimbursement_point_link_ending_date,
    v.venue_managing_offerer_id AS offerer_id,
FROM {{ source('raw', 'applicative_database_venue') }} AS V
LEFT JOIN  {{ source('raw', 'applicative_database_venue_reimbursement_point_link') }} AS vpl ON v.venue_id = vpl.venue_id
