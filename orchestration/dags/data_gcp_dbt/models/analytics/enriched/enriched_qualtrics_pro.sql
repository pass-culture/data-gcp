with
    topics as (
        select start_date, end_date, response_id, venue_id, answer as topics
        from {{ ref("qualtrics_answers_ir_pro") }}
        where question = 'Q1_topics'
    ),

    ir as (
        select
            pro.start_date,
            pro.end_date,
            pro.response_id,
            pro.venue_id,
            pro.user_type,
            pro.question,
            pro.answer,
            topics.topics,
            pro.anciennete_jours
        from {{ ref("qualtrics_answers_ir_pro") }} as pro
        left join
            topics
            on pro.response_id = topics.response_id
            and pro.venue_id = topics.venue_id
    ),

    ir_per_user as (select * from ir pivot (min(answer) for question in ('Q1', 'Q2'))),

    indiv_book as (
        select
            ir.response_id,
            booking.venue_id,
            count(distinct booking.booking_id) as individual_bookings,
            max(booking.booking_creation_date) as last_individual_booking
        from {{ ref("int_applicative__booking") }} as booking
        inner join
            ir_per_user as ir
            on booking.venue_id = ir.venue_id
            and booking.booking_creation_date <= date(ir.start_date)
        group by ir.response_id, booking.venue_id
    ),

    collective_book as (
        select
            ir.response_id,
            collective_booking.venue_id,
            count(
                distinct collective_booking.collective_booking_id
            ) as collective_bookings,
            max(
                collective_booking.collective_booking_creation_date
            ) as last_collective_booking
        from
            {{ source("raw", "applicative_database_collective_booking") }}
            as collective_booking
        inner join
            ir_per_user as ir
            on collective_booking.venue_id = ir.venue_id
            and collective_booking.collective_booking_creation_date
            <= date(ir.start_date)
        group by ir.response_id, collective_booking.venue_id
    ),

    indiv_offer as (
        select
            ir.response_id,
            offer.venue_id,
            count(distinct offer.offer_id) as individual_offers_created
        from {{ ref("int_raw__offer") }} as offer
        inner join
            ir_per_user as ir
            on offer.venue_id = ir.venue_id
            and offer.offer_creation_date <= date(ir.start_date)
        group by ir.response_id, offer.venue_id
    ),

    collective_offer as (
        select
            ir.response_id,
            collective_offer.venue_id,
            count(
                distinct collective_offer.collective_offer_id
            ) as collective_offers_created
        from {{ ref("int_applicative__collective_offer") }} as collective_offer
        inner join
            ir_per_user as ir
            on collective_offer.venue_id = ir.venue_id
            and collective_offer.collective_offer_creation_date <= date(ir.start_date)
        group by ir.response_id, collective_offer.venue_id
    ),

    first_dms as (
        select dms_pro.*, global_venue.venue_id
        from {{ ref("dms_pro") }} as dms_pro
        left join
            {{ ref("mrt_global__venue") }} as global_venue
            on dms_pro.demandeur_siret = global_venue.venue_siret
        where
            dms_pro.procedure_id in ('57081', '57189', '61589', '65028', '80264')
            and dms_pro.demandeur_siret <> 'nan'
            and global_venue.venue_id is not null
        qualify
            row_number() over (
                partition by dms_pro.demandeur_siret
                order by dms_pro.application_submitted_at asc
            )
            = 1
    ),

    first_dms_adage as (
        select
            ir.venue_id,
            ir.response_id,
            case
                when dms.demandeur_siret is null
                then 'dms_adage_non_depose'
                when dms.processed_at < timestamp(ir.start_date)
                then dms.application_status
                when dms.passed_in_instruction_at < timestamp(ir.start_date)
                then 'en_instruction'
                when dms.application_submitted_at < timestamp(ir.start_date)
                then 'en_construction'
                else 'dms_adage_non_depose'
            end as dms_adage_application_status
        from ir_per_user as ir
        left join first_dms as dms on ir.venue_id = dms.venue_id
    )

select
    ir.venue_id,
    ir.response_id,
    ir.topics,
    ir.q2 as commentaire,
    venue.venue_type_code as venue_type_label,
    venue_label.venue_label,
    venue.venue_department_code,
    indiv_book.last_individual_booking,
    collective_book.last_collective_booking,
    case
        when extract(day from date(ir.start_date)) >= 16
        then date_add(date_trunc(date(ir.start_date), month), interval 1 month)
        else date_trunc(date(ir.start_date), month)
    end as mois_releve,
    safe_cast(ir.q1 as int) as note,
    case
        when safe_cast(ir.q1 as int) <= 6
        then 'detracteur'
        when safe_cast(ir.q1 as int) < 9
        then 'neutre'
        else 'promoteur'
    end as user_satisfaction,
    coalesce(
        (
            lower(ir.topics) like '%inscription longue eac%'
            or lower(ir.topics) like '%référencement adage%'
        ),
        false
    ) as mauvaise_exp_adage,
    case
        when venue.venue_is_permanent
        then concat('venue-', venue.venue_id)
        else concat('offerer-', offerer.offerer_id)
    end as partner_id,
    coalesce(indiv_book.individual_bookings, 0) as individual_bookings,
    coalesce(collective_book.collective_bookings, 0) as collective_bookings,
    coalesce(indiv_book.individual_bookings, 0)
    + coalesce(collective_book.collective_bookings, 0) as total_bookings,
    coalesce(indiv_offer.individual_offers_created, 0) as individual_offers_created,
    coalesce(
        collective_offer.collective_offers_created, 0
    ) as collective_offers_created,
    coalesce(indiv_offer.individual_offers_created, 0)
    + coalesce(collective_offer.collective_offers_created, 0) as total_offers_created,
    coalesce(
        first_dms_adage.dms_adage_application_status, 'dms_adage_non_depose'
    ) as dms_adage_application_status
from ir_per_user as ir
left join indiv_book on ir.response_id = indiv_book.response_id
left join collective_book on ir.response_id = collective_book.response_id
left join indiv_offer on ir.response_id = indiv_offer.response_id
left join collective_offer on ir.response_id = collective_offer.response_id
left join
    first_dms_adage
    on ir.venue_id = first_dms_adage.venue_id
    and ir.response_id = first_dms_adage.response_id
left join
    {{ source("raw", "applicative_database_venue") }} as venue
    on ir.venue_id = venue.venue_id
left join
    {{ ref("int_raw__offerer") }} as offerer
    on venue.venue_managing_offerer_id = offerer.offerer_id
left join
    {{ source("raw", "applicative_database_venue_label") }} as venue_label
    on venue.venue_label_id = venue_label.venue_label_id
