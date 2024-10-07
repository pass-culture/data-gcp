with
    emails as (
        select *
        from
            external_query(
                '{{ applicative_external_connection_id }}',
                'SELECT CAST("id" AS varchar(255)) AS user_id, email FROM public.user'
            )
    )
select distinct
    template,
    tag,
    user_id,
    event_date,
    delivered_count,
    opened_count,
    unsubscribed_count,
    date("{{ ds }}") as execution_date
from
    `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(today()) }}_sendinblue_transactional_detailed_histo` s
left join emails on s.email = emails.email
