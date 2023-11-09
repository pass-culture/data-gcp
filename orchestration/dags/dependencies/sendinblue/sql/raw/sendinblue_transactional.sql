with emails as (
    SELECT
        *
    FROM EXTERNAL_QUERY(
        '{{ applicative_external_connection_id }}',
        'SELECT CAST("id" AS varchar(255)) AS user_id, email FROM public.user')
)
SELECT 
    template
    , tag
    , user_id
    , event_date
    , delivered_count
    , opened_count
    , unsubscribed_count
FROM `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(add_days(ds, -1)) }}_sendinblue_transactional_detailed_histo` s
LEFT JOIN emails
ON s.email = emails.email
