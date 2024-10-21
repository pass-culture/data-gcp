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
        select *
        from
            external_query(
                '{{ applicative_external_connection_id }}',
                'SELECT CAST("id" AS varchar(255)) AS venue_id, "bookingEmail" as venue_email FROM public.venue'
            )
    )
select distinct
    template,
    tag,
    case when venue_id is not null then venue_id else user_id end as user_id,
    event_date,
    delivered_count,
    opened_count,
    unsubscribed_count,
    date("{{ ds }}") as execution_date
from
    `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(today()) }}_sendinblue_transactional_detailed_histo` s
left join user_emails on s.email = user_emails.user_email
left join venue_emails on s.email = venue_emails.venue_email
