SELECT
    cb.collective_booking_id,
    cb.collective_stock_id,
    cs.stock_id,
    cs.collective_offer_id,
    cs.offer_id,
    cs.collective_offer_name,
    cs.collective_offer_subcategory_id,
    cs.collective_offer_category_id,
    cs.collective_offer_format,
    cs.venue_id,
    cs.venue_name,
    cs.venue_department_code,
    cs.venue_region_name,
    cb.offerer_id,
    cs.partner_id,
    cs.offerer_name,
    cs.collective_stock_price AS booking_amount, -- nom de cette colonne ? il faut que ce soit iso avec mrt_collective__offer
    cs.collective_stock_number_of_tickets,
    cs.collective_stock_beginning_date_time,
    cb.educational_institution_id,
    cb.educational_year_id,
    educational_year.scholar_year,
    cb.educational_redactor_id,
    eple.nom_etablissement,
    cs.institution_program_name,
    eple.code_departement AS school_department_code,
    school_region_departement.region_name AS school_region_name,
    eple.libelle_academie,
    cs.collective_offer_address_type,
    cb.collective_booking_creation_date,
    cb.collective_booking_cancellation_date,
    cb.collective_booking_is_cancelled,
    cb.collective_booking_status,
    cb.collective_booking_cancellation_reason,
    cb.collective_booking_confirmation_date,
    cb.collective_booking_confirmation_limit_date,
    cb.collective_booking_used_date,
    cb.collective_booking_reimbursement_date,
    cb.collective_booking_rank,
    cs.collective_offer_image_id,
FROM {{ ref('int_applicative__collective_booking') }}  AS cb
    INNER JOIN {{ ref('mrt_collective__stock') }} AS cs ON cs.collective_stock_id = cb.collective_stock_id
    INNER JOIN {{ source('raw', 'applicative_database_educational_year') }} AS educational_year ON educational_year.adage_id = cb.educational_year_id
    INNER JOIN {{ ref('educational_institution') }} AS educational_institution ON educational_institution.educational_institution_id = cb.educational_institution_id
    LEFT JOIN {{ source('analytics', 'eple') }} AS eple ON eple.id_etablissement = educational_institution.institution_id
    LEFT JOIN {{ source('analytics', 'region_department') }} AS school_region_departement ON eple.code_departement = school_region_departement.num_dep