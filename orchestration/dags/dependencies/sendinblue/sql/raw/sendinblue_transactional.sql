with
    user_emails as (
        select *
        from
            external_query(
                '{{ applicative_external_connection_id }}',
                'SELECT CAST("id" AS varchar(255)) AS user_id, "email" as user_email FROM public.user'
            )
    ),
    venue_emails as (
        select
            venue_managing_offerer_id as offerer_id,
            venue_id,
            venue_booking_email as venue_email
        from `{{ bigquery_raw_dataset }}.applicative_database_venue`
    ),
    collective_offer_emails as (
        select distinct venue_id, booking_email as collective_offer_email
        from
            (
                select
                    venue_id, split(collective_offer_booking_email, ',') as emails_array
                from `{{ bigquery_raw_dataset }}.applicative_database_collective_offer`
            ),
            unnest(emails_array) as booking_email
    ),
    collective_offer_emails_offerer as (
        select offerer_id, collective_offer_email
        from collective_offer_emails
        left join
            venue_emails on collective_offer_emails.venue_id = venue_emails.venue_id
    )
select distinct
    template,
    tag,
    user_id,
    target,
    coalesce(venue_emails.offerer_id, co.offerer_id) as offerer_id,
    event_date,
    delivered_count,
    opened_count,
    unsubscribed_count,
    date("{{ ds }}") as execution_date
from
    `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(today()) }}_sendinblue_transactional_detailed_histo` s
left join user_emails on s.email = user_emails.user_email
left join venue_emails on s.email = venue_emails.venue_email
left join collective_offer_emails_offerer as co on s.email = co.collective_offer_email
