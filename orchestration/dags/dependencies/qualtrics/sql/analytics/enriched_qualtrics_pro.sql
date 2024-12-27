with
    topics as (
        select start_date, end_date, response_id, venue_id, answer as topics
        from `{{ bigquery_analytics_dataset }}.qualtrics_answers_ir_pro`
        where question = "Q1_topics"
    ),

    ir as (
        select
            pro.start_date,
            pro.end_date,
            pro.response_id,
            pro.venue_id,
            user_type,
            question,
            answer,
            topics,
            anciennete_jours
        from `{{ bigquery_analytics_dataset }}.qualtrics_answers_ir_pro` pro
        left join
            topics
            on pro.response_id = topics.response_id
            and pro.venue_id = topics.venue_id
    ),
    ir_per_user as (select * from ir pivot (min(answer) for question in ('Q1', 'Q2'))),
    indiv_book as (
        select
            response_id,
            booking.venue_id,
            count(distinct booking_id) individual_bookings,
            max(booking_creation_date) last_individual_booking
        from `{{ bigquery_raw_dataset }}.applicative_database_booking` as booking
        join
            ir_per_user
            on ir_per_user.venue_id = booking.venue_id
            and booking.booking_creation_date <= date(ir_per_user.start_date)
        group by 1, 2
    ),
    collective_book as (
        select
            response_id,
            collective_booking.venue_id,
            count(distinct collective_booking_id) collective_bookings,
            max(collective_booking_creation_date) last_collective_booking
        from
            `{{ bigquery_clean_dataset }}.applicative_database_collective_booking`
            as collective_booking
        join
            ir_per_user
            on ir_per_user.venue_id = collective_booking.venue_id
            and collective_booking.collective_booking_creation_date
            <= date(ir_per_user.start_date)
        group by 1, 2
        order by 3
    ),
    indiv_offer as (
        select
            response_id,
            offer.venue_id,
            count(distinct offer_id) individual_offers_created
        from `{{ bigquery_clean_dataset }}.applicative_database_offer` as offer
        join
            ir_per_user
            on ir_per_user.venue_id = offer.venue_id
            and offer.offer_creation_date <= date(ir_per_user.start_date)
        group by 1, 2
    ),
    collective_offer as (
        select
            response_id,
            collective_offer.venue_id,
            count(distinct collective_offer_id) collective_offers_created
        from
            `{{ bigquery_raw_dataset }}.applicative_database_collective_offer`
            as collective_offer
        join
            ir_per_user
            on ir_per_user.venue_id = collective_offer.venue_id
            and collective_offer.collective_offer_creation_date
            <= date(ir_per_user.start_date)
        group by 1, 2
    ),
    first_dms as (
        select global_venue.venue_id, dms_pro.*
        from {{ bigquery_analytics_dataset }}.dms_pro
        left join
            {{ bigquery_analytics_dataset }}.global_venue
            on dms_pro.demandeur_siret = global_venue.venue_siret
        where
            procedure_id in ('57081', '57189', '61589', '65028', '80264')
            and demandeur_siret <> "nan"
            and venue_id is not null
        qualify
            row_number() over (
                partition by demandeur_siret order by application_submitted_at asc
            )
            = 1
    ),
    first_dms_adage as (
        select
            ir_per_user.venue_id,
            ir_per_user.response_id,
            case
                when first_dms.demandeur_siret is null
                then "dms_adage_non_depose"
                when first_dms.processed_at < timestamp(start_date)
                then application_status
                when first_dms.passed_in_instruction_at < timestamp(start_date)
                then "en_instruction"
                when first_dms.application_submitted_at < timestamp(start_date)
                then "en_construction"
                else "dms_adage_non_depose"
            end as dms_adage_application_status
        from ir_per_user
        left join first_dms on ir_per_user.venue_id = first_dms.venue_id

    )

select
    case
        when extract(day from date(start_date)) >= 16
        then date_add(date_trunc(date(start_date), month), interval 1 month)
        else date_trunc(date(start_date), month)
    end as mois_releve,
    ir_per_user.venue_id,
    ir_per_user.response_id,
    safe_cast(ir_per_user.q1 as int) as note,
    ir_per_user.topics.topics as topics,
    case
        when safe_cast(ir_per_user.q1 as int) <= 6
        then "detracteur"
        when safe_cast(ir_per_user.q1 as int) < 9
        then "neutre"
        else "promoteur"
    end as user_satisfaction,
    ir_per_user.q2 as commentaire,
    case
        when
            (
                lower(ir_per_user.topics.topics) like "%inscription longue eac%"
                or lower(ir_per_user.topics.topics) like "%référencement adage%"
            )
        then true
        else false
    end as mauvaise_exp_adage,
    case
        when venue.venue_is_permanent
        then concat("venue-", venue.venue_id)
        else concat("offerer-", offerer.offerer_id)
    end as partner_id,
    venue.venue_type_code as venue_type_label,
    venue_label.venue_label as venue_label,
    venue.venue_department_code,
    ifnull(indiv_book.individual_bookings, 0) individual_bookings,
    indiv_book.last_individual_booking,
    ifnull(collective_book.collective_bookings, 0) collective_bookings,
    collective_book.last_collective_booking,
    ifnull(indiv_book.individual_bookings, 0)
    + ifnull(collective_book.collective_bookings, 0) total_bookings,
    ifnull(indiv_offer.individual_offers_created, 0) individual_offers_created,
    ifnull(collective_offer.collective_offers_created, 0) collective_offers_created,
    ifnull(indiv_offer.individual_offers_created, 0)
    + ifnull(collective_offer.collective_offers_created, 0) total_offers_created,
    ifnull(
        dms_adage_application_status, "dms_adage_non_depose"
    ) dms_adage_application_status
from ir_per_user
left join indiv_book on ir_per_user.response_id = indiv_book.response_id
left join collective_book on ir_per_user.response_id = collective_book.response_id
left join indiv_offer on ir_per_user.response_id = indiv_offer.response_id
left join collective_offer on ir_per_user.response_id = collective_offer.response_id
left join
    first_dms_adage
    on ir_per_user.venue_id = first_dms_adage.venue_id
    and ir_per_user.response_id = first_dms_adage.response_id
left join
    `{{ bigquery_raw_dataset }}.applicative_database_venue` venue
    on ir_per_user.venue_id = venue.venue_id
left join
    `{{ bigquery_int_raw_dataset }}.offerer` as offerer
    on venue.venue_managing_offerer_id = offerer.offerer_id
left join
    `{{ bigquery_raw_dataset }}.applicative_database_venue_label` as venue_label
    on venue_label.venue_label_id = venue.venue_label_id
