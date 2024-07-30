with fraud_users as (
    select user_id
    from {{ ref('user_suspension') }}
    qualify
        ROW_NUMBER() over (partition by user_id order by action_date desc) = 1 -- ceux qui n'ont pas été unsuspended ensuite
        and action_type = 'USER_SUSPENDED'
        and action_history_json_data like '%fraud%'
),


individual_offers_created as (
    select
        enriched_cultural_partner_data.partner_id,
        enriched_cultural_partner_data.partner_type,
        enriched_cultural_partner_data.cultural_sector,
        COUNT(offer_id) as individual_offers_created_cnt,
        COUNT(case when DATE_DIFF(CURRENT_DATE, offer_creation_date, month) <= 2 then offer_id end) as individual_offers_created_last_2_month,
        COUNT(case when DATE_DIFF(CURRENT_DATE, offer_creation_date, month) <= 6 then offer_id end) as individual_offers_created_last_6_month,
        COUNT(case when DATE_DIFF(enriched_cultural_partner_data.last_bookable_offer_date, offer_creation_date, month) <= 2 then offer_id end) as individual_offers_created_2_month_before_last_bookable,
        COUNT(case when DATE_DIFF(enriched_cultural_partner_data.last_bookable_offer_date, offer_creation_date, month) <= 6 then offer_id end) as individual_offers_created_6_month_before_last_bookable
    from {{ ref('enriched_cultural_partner_data') }} as enriched_cultural_partner_data
        join {{ ref('partner_type_bookability_frequency') }} using (partner_type)
        left join {{ ref('mrt_global__offer') }} as mrt_global__offer on enriched_cultural_partner_data.partner_id = mrt_global__offer.partner_id
    group by 1, 2, 3
),

individual_bookings as (
    select
        enriched_cultural_partner_data.partner_id,
        enriched_cultural_partner_data.partner_type,
        enriched_cultural_partner_data.cultural_sector,
        COALESCE(COUNT(distinct user_id), 0) as unique_users,
        COALESCE(COUNT(distinct case when mrt_global__booking.user_id in (select distinct user_id from fraud_users) then mrt_global__booking.user_id else NULL end), 0) as unique_fraud_users,
        COALESCE(COUNT(booking_id), 0) as individual_bookings_cnt,
        COALESCE(SUM(case when booking_is_used then booking_intermediary_amount else NULL end), 0) as real_individual_revenue,
        COALESCE(COUNT(case when DATE_DIFF(CURRENT_DATE, booking_creation_date, month) <= 2 then booking_id end), 0) as individual_bookings_last_2_month,
        COALESCE(COUNT(case when DATE_DIFF(CURRENT_DATE, booking_creation_date, month) <= 6 then booking_id end), 0) as individual_bookings_last_6_month,
        COALESCE(COUNT(case when DATE_DIFF(last_bookable_offer_date, booking_creation_date, month) <= 2 then booking_id end), 0) as individual_bookings_2_month_before_last_bookable,
        COALESCE(COUNT(case when DATE_DIFF(last_bookable_offer_date, booking_creation_date, month) <= 6 then booking_id end), 0) as individual_bookings_6_month_before_last_bookable
    from {{ ref('enriched_cultural_partner_data') }}
        join {{ ref('partner_type_bookability_frequency') }} using (partner_type)
        left join {{ ref('mrt_global__booking') }}
            as mrt_global__booking on enriched_cultural_partner_data.partner_id = mrt_global__booking.partner_id
        and not booking_is_cancelled
    group by 1, 2, 3
),

collective_offers_created as (
    select
        enriched_cultural_partner_data.partner_id,
        enriched_cultural_partner_data.partner_type,
        enriched_cultural_partner_data.cultural_sector,
        COALESCE(COUNT(collective_offer_id), 0) as collective_offers_created_cnt,
        COALESCE(COUNT(case when DATE_DIFF(CURRENT_DATE, collective_offer_creation_date, month) <= 2 then collective_offer_id end), 0) as collective_offers_created_last_2_month,
        COALESCE(COUNT(case when DATE_DIFF(CURRENT_DATE, collective_offer_creation_date, month) <= 6 then collective_offer_id end), 0) as collective_offers_created_last_6_month,
        COALESCE(COUNT(case when DATE_DIFF(last_bookable_offer_date, collective_offer_creation_date, month) <= 2 then collective_offer_id end), 0) as collective_offers_created_2_month_before_last_bookable,
        COALESCE(COUNT(case when DATE_DIFF(last_bookable_offer_date, collective_offer_creation_date, month) <= 6 then collective_offer_id end), 0) as collective_offers_created_6_month_before_last_bookable
    from {{ ref('enriched_cultural_partner_data') }}
        join {{ ref('partner_type_bookability_frequency') }} using (partner_type)
        left join {{ ref('enriched_collective_offer_data') }} on enriched_cultural_partner_data.partner_id = enriched_collective_offer_data.partner_id
    group by 1, 2, 3
),

collective_bookings as (
    select
        enriched_cultural_partner_data.partner_id,
        enriched_cultural_partner_data.partner_type,
        enriched_cultural_partner_data.cultural_sector,
        COALESCE(COUNT(collective_booking_id), 0) as collective_bookings_cnt,
        COALESCE(COUNT(case when DATE_DIFF(CURRENT_DATE, collective_booking_creation_date, month) <= 2 then collective_booking_id end), 0) as collective_bookings_last_2_month,
        COALESCE(COUNT(case when DATE_DIFF(CURRENT_DATE, collective_booking_creation_date, month) <= 6 then collective_booking_id end), 0) as collective_bookings_last_6_month,
        COALESCE(COUNT(case when DATE_DIFF(last_bookable_offer_date, collective_booking_creation_date, month) <= 2 then collective_booking_id end), 0) as collective_bookings_2_month_before_last_bookable,
        COALESCE(COUNT(case when DATE_DIFF(CURRENT_DATE, collective_booking_creation_date, month) <= 6 then collective_booking_id end), 0) as collective_bookings_6_month_before_last_bookable,
        COALESCE(SUM(case when collective_booking_status in ('USED', 'REIMBURSED') then booking_amount else NULL end), 0) as real_collective_revenue
    from {{ ref('enriched_cultural_partner_data') }}
        join {{ ref('partner_type_bookability_frequency') }} using (partner_type)
        left join {{ ref('enriched_collective_booking_data') }}
            collective_booking on enriched_cultural_partner_data.partner_id = collective_booking.partner_id
        and not collective_booking_status = 'CANCELLED'
    group by 1, 2, 3
),

favorites1 as (
    select distinct
        case
            when mrt_global__venue.venue_is_permanent then CONCAT("venue-", mrt_global__venue.venue_id)
            else CONCAT("offerer-", mrt_global__venue.venue_managing_offerer_id)
        end as partner_id,
        applicative_database_favorite.*
    from {{ ref('mrt_global__venue') }} as mrt_global__venue
        left join {{ ref('mrt_global__offer') }} as mrt_global__offer on mrt_global__venue.venue_id = mrt_global__offer.venue_id
        left join {{ ref('favorite') }} on mrt_global__offer.offer_id = applicative_database_favorite.offerid
),

favorites as (
    select
        enriched_cultural_partner_data.partner_id,
        enriched_cultural_partner_data.partner_type,
        enriched_cultural_partner_data.cultural_sector,
        COALESCE(COUNT(*), 0) as favorites_cnt
    from favorites1
        join {{ ref('enriched_cultural_partner_data') }} using (partner_id)
        join {{ ref('partner_type_bookability_frequency') }} on enriched_cultural_partner_data.partner_type = partner_type_bookability_frequency.partner_type
    group by 1, 2, 3
),

consultations as (
    select
        case
            when venue.venue_is_permanent then CONCAT("venue-", venue.venue_id)
            else CONCAT("offerer-", venue.venue_managing_offerer_id)
        end as partner_id,
        SUM(cnt_events) as total_consultation,
        COALESCE(SUM(case when DATE_DIFF(CURRENT_DATE, event_date, month) <= 2 then cnt_events end)) as consult_last_2_month,
        COALESCE(SUM(case when DATE_DIFF(CURRENT_DATE, event_date, month) <= 6 then cnt_events end)) as consult_last_6_month,
        COALESCE(SUM(case when DATE_DIFF(venue.last_bookable_offer_date, event_date, month) <= 2 then cnt_events end)) as consult_2_month_before_last_bookable,
        COALESCE(SUM(case when DATE_DIFF(venue.last_bookable_offer_date, event_date, month) <= 6 then cnt_events end)) as consult_6_month_before_last_bookable
    from {{ ref('aggregated_daily_offer_consultation_data') }} consult
        left join {{ ref('mrt_global__venue') }} venue on consult.venue_id = venue.venue_id
        left join {{ ref('enriched_cultural_partner_data') }} on (case when venue.venue_is_permanent then CONCAT("venue-", venue.venue_id) else CONCAT("offerer-", venue.venue_managing_offerer_id) end) = enriched_cultural_partner_data.partner_id
    group by 1
),

adage_status as (
    select distinct
        case
            when mrt_global__venue.venue_is_permanent then CONCAT("venue-", mrt_global__venue.venue_id)
            else CONCAT("offerer-", mrt_global__venue.venue_managing_offerer_id)
        end as partner_id,
        enriched_offerer_data.first_dms_adage_status
    from {{ ref('mrt_global__venue') }} as mrt_global__venue
        left join {{ ref('enriched_offerer_data') }} on mrt_global__venue.venue_managing_offerer_id = enriched_offerer_data.offerer_id
),

siren_status as (
    select distinct
        mrt_global__venue.partner_id,
        case when etatadministratifunitelegale = 'A' then TRUE else FALSE end as has_active_siren
    from {{ ref('mrt_global__venue') }} as mrt_global__venue
        join {{ ref('enriched_offerer_data') }} on mrt_global__venue.venue_managing_offerer_id = enriched_offerer_data.offerer_id
        left join {{ ref('siren_data') }} on enriched_offerer_data.offerer_siren = siren_data.siren
),

rejected_offers as (
    select
        case
            when mrt_global__venue.venue_is_permanent then CONCAT("venue-", mrt_global__venue.venue_id)
            else CONCAT("offerer-", mrt_global__venue.venue_managing_offerer_id)
        end as partner_id,
        COALESCE(COUNT(*), 0) as offers_cnt
    from {{ ref('mrt_global__venue') }} as mrt_global__venue
        left join {{ ref('offer') }} on applicative_database_offer.venue_id = mrt_global__venue.venue_id
    where offer_validation = 'REJECTED'
    group by 1
),

providers as (
    select
        case
            when mrt_global__venue.venue_is_permanent then CONCAT("venue-", mrt_global__venue.venue_id)
            else CONCAT("offerer-", mrt_global__venue.venue_managing_offerer_id)
        end as partner_id,
        case when provider_id is not NULL then TRUE else FALSE end as has_provider
    from {{ ref('mrt_global__venue') }} as mrt_global__venue
        left join {{ ref('mrt_global__venue_provider') }} as mrt_global__venue_provider on mrt_global__venue_provider.venue_id = mrt_global__venue.venue_id and is_active
),

--- On estime que si une structure a un lieu rattaché à un point de remboursement, tous les lieux de la structure le sont
reimbursment_point1 as (
    select distinct
        mrt_global__venue.venue_managing_offerer_id as offerer_id,
        mrt_global__venue.venue_id,
        venue_is_permanent,
        reimbursement_point_link_beginning_date,
        reimbursement_point_link_ending_date,
        RANK() over (partition by mrt_global__venue.venue_managing_offerer_id, mrt_global__venue.venue_id order by reimbursement_point_link_beginning_date desc) as rang
    from {{ ref('mrt_global__venue') }} as mrt_global__venue
        left join {{ ref('venue_reimbursement_point_link') }} on mrt_global__venue.venue_id = applicative_database_venue_reimbursement_point_link.venue_id
),

reimbursment_point2 as (
    select
        offerer_id,
        venue_id,
        venue_is_permanent,
        COALESCE(COUNT(case when reimbursement_point_link_beginning_date is not NULL then 1 else 0 end)) as nb_reimbursment_point
    from reimbursment_point1
    where
        rang = 1
        and reimbursement_point_link_ending_date is NULL
    group by 1, 2, 3
),


reimbursment_point as (
    select
        case
            when venue_is_permanent then CONCAT("venue-", venue_id)
            else CONCAT("offerer-", offerer_id)
        end as partner_id,
        SUM(nb_reimbursment_point) as nb_reimbursment_point
    from reimbursment_point2
    group by 1
),

bookable as (
    select
        case
            when mrt_global__venue.venue_is_permanent then CONCAT("venue-", bookable_venue_history.venue_id)
            else CONCAT("offerer-", bookable_venue_history.offerer_id)
        end as partner_id,
        MAX(partition_date) last_bookable_date
    from {{ ref('bookable_venue_history') }} as bookable_venue_history
        left join {{ ref('mrt_global__venue') }} as mrt_global__venue on bookable_venue_history.venue_id = mrt_global__venue.venue_id
    where bookable_venue_history.total_bookable_offers <> 0
    group by 1
),

churned as (
    select
        bookable.partner_id,
        last_bookable_date,
        enriched_cultural_partner_data.cultural_sector,
        median_bookability_frequency,
        DATE_DIFF(CURRENT_DATE(), last_bookable_date, day) days_since_last_bookable_offer
    from bookable
        join {{ ref('enriched_cultural_partner_data') }} on bookable.partner_id = enriched_cultural_partner_data.partner_id
        join {{ ref('cultural_sector_bookability_frequency') }} on enriched_cultural_partner_data.cultural_sector = cultural_sector_bookability_frequency.cultural_sector
),

churn_segmentation as (
    select
        partner_id,
        last_bookable_date,
        cultural_sector,
        days_since_last_bookable_offer,
        case
            when median_bookability_frequency = 13
                then
                    case
                        when days_since_last_bookable_offer < 30 then "active"
                        when days_since_last_bookable_offer < 60 then "at_risk"
                        else "churned"
                    end
            when median_bookability_frequency > 6
                then
                    case
                        when days_since_last_bookable_offer < 60 then "active"
                        when days_since_last_bookable_offer < 120 then "at_risk"
                        else "churned"
                    end
            when median_bookability_frequency <= 6
                then
                    case
                        when days_since_last_bookable_offer < 90 then "active"
                        when days_since_last_bookable_offer < 180 then "at_risk"
                        else "churned"
                    end
            else "not-activated"
        end as partner_segmentation
    from churned
)

select distinct
    enriched_cultural_partner_data.partner_id,
    enriched_cultural_partner_data.partner_creation_date,
    DATE_DIFF(CURRENT_DATE, partner_creation_date, month) as seniority_month,
    enriched_cultural_partner_data.cultural_sector,
    enriched_cultural_partner_data.partner_type,
    case when enriched_cultural_partner_data.individual_offers_created > 0 then TRUE else FALSE end as activated_individual_part,
    case when enriched_cultural_partner_data.collective_offers_created > 0 then TRUE else FALSE end as activated_collective_part,
    COALESCE(individual_offers_created_cnt, 0) as individual_offers_created_cnt,
    COALESCE(individual_offers_created_last_2_month, 0) as individual_offers_created_last_2_month,
    COALESCE(individual_offers_created_last_6_month, 0) as individual_offers_created_last_6_month,
    COALESCE(individual_offers_created_2_month_before_last_bookable, 0) as individual_offers_created_2_month_before_last_bookable,
    COALESCE(individual_offers_created_6_month_before_last_bookable, 0) as individual_offers_created_6_month_before_last_bookable,
    COALESCE(collective_offers_created_cnt, 0) as collective_offers_created_cnt,
    COALESCE(collective_offers_created_last_2_month, 0) as collective_offers_created_last_2_month,
    COALESCE(collective_offers_created_last_6_month, 0) as collective_offers_created_last_6_month,
    COALESCE(collective_offers_created_2_month_before_last_bookable, 0) as collective_offers_created_2_month_before_last_bookable,
    COALESCE(collective_offers_created_6_month_before_last_bookable, 0) as collective_offers_created_6_month_before_last_bookable,
    COALESCE(individual_bookings_cnt, 0) as individual_bookings_cnt,
    COALESCE(individual_bookings_last_2_month, 0) as individual_bookings_last_2_month,
    COALESCE(individual_bookings_last_6_month, 0) as individual_bookings_last_6_month,
    COALESCE(individual_bookings_2_month_before_last_bookable, 0) as individual_bookings_2_month_before_last_bookable,
    COALESCE(individual_bookings_6_month_before_last_bookable, 0) as individual_bookings_6_month_before_last_bookable,
    COALESCE(collective_bookings_cnt, 0) as collective_bookings_cnt,
    COALESCE(collective_bookings_last_2_month, 0) as collective_bookings_last_2_month,
    COALESCE(collective_bookings_last_6_month, 0) as collective_bookings_last_6_month,
    COALESCE(collective_bookings_2_month_before_last_bookable, 0) as collective_bookings_2_month_before_last_bookable,
    COALESCE(collective_bookings_6_month_before_last_bookable, 0) as collective_bookings_6_month_before_last_bookable,
    COALESCE(individual_bookings.real_individual_revenue, 0) as real_individual_revenue,
    COALESCE(collective_bookings.real_collective_revenue, 0) as real_collective_revenue,
    COALESCE(favorites.favorites_cnt, 0) as favorites_cnt,
    COALESCE(consultations.total_consultation, 0) as total_consultation,
    COALESCE(consultations.consult_last_2_month, 0) as consultation_last_2_month,
    COALESCE(consultations.consult_last_6_month, 0) as consultation_last_6_month,
    COALESCE(consultations.consult_2_month_before_last_bookable, 0) as consultation_2_month_before_last_bookable,
    COALESCE(consultations.consult_6_month_before_last_bookable, 0) as consultation_6_month_before_last_bookable,
    has_active_siren,
    COALESCE(first_dms_adage_status, "dms_adage_non_depose") as first_dms_adage_status,
    COALESCE(rejected_offers.offers_cnt, 0) as rejected_offers_cnt,
    COALESCE(ROUND(SAFE_DIVIDE(rejected_offers.offers_cnt, individual_offers_created_cnt + rejected_offers.offers_cnt) * 100), 0) as rejected_offers_pct,
    has_provider,
    case when nb_reimbursment_point >= 1 then TRUE else FALSE end as has_reimbursement_point,
    COALESCE(unique_fraud_users, 0) as unique_fraud_users,
    ROUND(SAFE_DIVIDE(COALESCE(unique_fraud_users, 0), COALESCE(unique_users, 0)) * 100) as pct_unique_fraud_users,
    days_since_last_bookable_offer,
    COALESCE(partner_segmentation, "not activated") partner_segmentation
from {{ ref('enriched_cultural_partner_data') }}
    left join individual_offers_created on enriched_cultural_partner_data.partner_id = individual_offers_created.partner_id
    left join collective_offers_created on enriched_cultural_partner_data.partner_id = collective_offers_created.partner_id
    left join individual_bookings on enriched_cultural_partner_data.partner_id = individual_bookings.partner_id
    left join collective_bookings on enriched_cultural_partner_data.partner_id = collective_bookings.partner_id
    left join favorites on enriched_cultural_partner_data.partner_id = favorites.partner_id
    left join siren_status on enriched_cultural_partner_data.partner_id = siren_status.partner_id
    left join rejected_offers on enriched_cultural_partner_data.partner_id = rejected_offers.partner_id
    left join providers on enriched_cultural_partner_data.partner_id = providers.partner_id
    left join reimbursment_point on enriched_cultural_partner_data.partner_id = reimbursment_point.partner_id
    left join consultations on enriched_cultural_partner_data.partner_id = consultations.partner_id
    left join adage_status on enriched_cultural_partner_data.partner_id = adage_status.partner_id
    left join churn_segmentation on enriched_cultural_partner_data.partner_id = churn_segmentation.partner_id
