with
    collective_offer_status as (
        select
            venue_id,
            count(
                distinct case
                    when
                        collective_offer_is_active = true
                        and collective_offer_validation = 'APPROVED'
                    then collective_offer_id
                end
            ) as total_active_collective_offers,
            count(
                distinct case
                    when
                        collective_offer_is_active = true
                        and collective_offer_validation = 'PENDING'
                    then collective_offer_id
                end
            ) as total_pending_collective_offers,
            count(
                distinct case
                    when
                        collective_offer_is_active = false
                        and collective_offer_validation != 'REJECTED'
                    then collective_offer_id
                end
            ) as total_inactive_non_rejected_collective_offers
        from {{ ref("int_global__collective_offer") }}
        group by venue_id
    ),

    individual_offer_status as (
        select
            o.venue_id,
            count(
                distinct case
                    when
                        o.is_active = true
                        and o.offer_validation = 'APPROVED'
                        and s.is_bookable = true
                    then o.offer_id
                end
            ) as total_active_offers,
            count(
                distinct case
                    when o.is_active = true and o.offer_validation = 'PENDING'
                    then o.offer_id
                end
            ) as total_pending_offers,
            count(
                distinct case
                    when o.is_active = false and o.offer_validation != 'REJECTED'
                    then o.offer_id
                end
            ) as total_inactive_non_rejected_offers
        from {{ ref("int_global__offer") }} as o
        left join
            {{ ref("int_global__stock") }} as s
            on o.offer_id = s.offer_id
            and s.stock_is_soft_deleted = false
        group by o.venue_id
    )

select
    v.venue_id,
    coalesce(ios.total_active_offers, 0) as total_active_offers,
    coalesce(ios.total_pending_offers, 0) as total_pending_offers,
    coalesce(
        ios.total_inactive_non_rejected_offers, 0
    ) as total_inactive_non_rejected_offers,
    coalesce(cos.total_active_collective_offers, 0) as total_active_collective_offers,
    coalesce(cos.total_pending_collective_offers, 0) as total_pending_collective_offers,
    coalesce(
        cos.total_inactive_non_rejected_collective_offers, 0
    ) as total_inactive_non_rejected_collective_offers
from {{ ref("int_global__venue") }} as v
left join individual_offer_status as ios on v.venue_id = ios.venue_id
left join collective_offer_status as cos on v.venue_id = cos.venue_id
