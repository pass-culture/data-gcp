select
    btp.brevo_tag,
    btp.offerer_id,
    go.offerer_name,
    go.last_individual_offer_creation_date,
    go.last_collective_offer_creation_date,
    go.last_individual_bookable_offer_date,
    go.last_collective_bookable_offer_date,
    go.last_individual_booking_date,
    go.last_collective_booking_date,
    date(go.offerer_creation_date) as offerer_creation_date,
    min(btp.event_date) as first_open_date,
    max(btp.event_date) as last_open_date,
    count(
        distinct case when btp.email_is_opened then btp.event_date end
    ) as nb_open_days,
    min(
        case when bvh.total_individual_bookable_offers > 0 then bvh.partition_date end
    ) as first_individual_bookable_date_after_mail,
    min(
        case when bvh.total_collective_bookable_offers > 0 then bvh.partition_date end
    ) as first_collective_bookable_date_after_mail
from {{ ref("mrt_marketing__transactional_pro") }} as btp
left join {{ ref("int_global__offerer") }} as go on btp.offerer_id = go.offerer_id
left join {{ ref("mrt_global__venue") }} as gv on btp.offerer_id = gv.offerer_id
left join
    {{ ref("int_history__bookable_venue") }} as bvh
    on gv.venue_id = bvh.venue_id
    and btp.event_date <= bvh.partition_date
where
    btp.offerer_id is not null
    and btp.email_is_opened
    and (btp.brevo_tag like "%retention%" or btp.brevo_tag like "%inactiv%")
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
