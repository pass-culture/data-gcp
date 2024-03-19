{% set target_name = target.name %}

{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}

SELECT
    v.venue_thumb_count,
    v.venue_address,
    v.venue_postal_code,
    v.venue_city,
    v.ban_id,
    v.venue_id,
    v.venue_name,
    v.venue_siret,
    v.venue_latitude,
    v.venue_longitude,
    v.venue_managing_offerer_id,
    v.venue_booking_email,
    v.venue_is_virtual,
    v.venue_comment,
    v.venue_public_name,
    v.venue_type_code,
    v.venue_label_id,
    v.venue_creation_date,
    v.venue_is_permanent,
    v.banner_url,
    v.venue_audioDisabilityCompliant,
    v.venue_mentalDisabilityCompliant,
    v.venue_motorDisabilityCompliant,
    v.venue_visualDisabilityCompliant,
    v.venue_adage_id,
    v.venue_educational_status_id,
    v.collective_description,
    v.collective_students,
    v.collective_website,
    v.collective_network,
    v.collective_intervention_area,
    v.collective_access_information,
    v.collective_phone,
    v.collective_email,
    v.dms_token,
    v.venue_description,
    v.venue_withdrawal_details,
    COALESCE(CASE WHEN v.venue_postal_code = "97150" THEN "978"
        WHEN SUBSTRING(v.venue_postal_code, 0, 2) = "97" THEN SUBSTRING(v.venue_postal_code, 0, 3)
        WHEN SUBSTRING(v.venue_postal_code, 0, 2) = "98" THEN SUBSTRING(v.venue_postal_code, 0, 3)
        WHEN SUBSTRING(v.venue_postal_code, 0, 3) in ("200", "201", "209", "205") THEN "2A"
        WHEN SUBSTRING(v.venue_postal_code, 0, 3) in ("202", "206") THEN "2B"
        ELSE SUBSTRING(v.venue_postal_code, 0, 2)
        END,
        v.venue_department_code
    ) AS venue_department_code,
    CONCAT(
        "https://backoffice.passculture.team/pro/venue/",
        v.venue_id
    ) AS venue_backoffice_link,
    CONCAT(
        "https://passculture.pro/structures/",
        o.offerer_humanized_id,
        "/lieux/",
        "humanize_id(venue_id)"
    ) AS venue_pc_pro_link,
    null as humanized_id, -- todo : check la macro
    vr.venue_target AS venue_targeted_audience,
    vc.venue_contact_phone_number,
    vc.venue_contact_email,
    vc.venue_contact_website,
    vl.venue_label,
    o.offerer_id,
    o.offerer_name,
    o.offerer_validation_status,
    o.offerer_is_active
FROM {{ source("raw", "applicative_database_venue") }} AS v
LEFT JOIN {{ source("raw", "applicative_database_venue_registration") }} AS vr ON v.venue_id = vr.venue_id
LEFT JOIN {{ source("raw", "applicative_database_venue_contact") }} AS vc ON v.venue_id = vc.venue_id
LEFT JOIN{{ source('raw', 'applicative_database_venue_label') }} AS vl ON vl.venue_label_id = v.venue_label_id
LEFT JOIN {{ ref("int_applicative__offerer") }} AS o ON v.venue_managing_offerer_id = o.offerer_id
