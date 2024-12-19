SELECT
    btp.offerer_id,
    btp.brevo_tag,
    go.offerer_name,
    btp.event_date,
    go.last_individual_offer_creation_date,
    go.last_collective_offer_creation_date,
    go.last_individual_bookable_offer_date,
    go.last_collective_bookable_offer_date,
    go.last_individual_booking_date,
    go.last_collective_booking_date,
    DATE(go.offerer_creation_date) AS offerer_creation_date,
    MIN(btp.event_date) AS first_open_date,
    MAX(btp.event_date) AS last_open_date,
    COUNT(DISTINCT CASE WHEN btp.email_is_opened THEN btp.event_date END) AS nb_open_days,
    MIN(CASE WHEN btp.individual_bookable_offers > 0 THEN bvh.partition_date END) AS first_individual_bookable_date_after_mail,
    MIN(CASE WHEN btp.collective_bookable_offers > 0 THEN bvh.partition_date END) AS first_collective_bookable_date_after_mail
FROM {{ ref("mrt_brevo__transactional_pro") }} AS btp
LEFT JOIN {{ ref("int_global__offerer") }} AS go ON btp.offerer_id = go.offerer_id
LEFT JOIN {{ ref("mrt_global__venue") }} AS gv ON btp.offerer_id = gv.venue_managing_offerer_id
LEFT JOIN {{ ref("bookable_venue_history") }} AS bvh ON gv.venue_id = bvh.venue_id
    AND btp.event_date <= bvh.partition_date
WHERE btp.offerer_id IS NOT null
    AND btp.email_is_opened
    AND (btp.brevo_tag LIKE "%retention%"
    OR btp.brevo_tag LIKE "%inactiv%")
GROUP BY
    1,2,3,4,5,6,7,8,9,10
