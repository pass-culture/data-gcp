with
    user_infos as (
        select
            tag,
            template,
            user_id,
            sum(delivered_count) as delivered_count,
            sum(opened_count) as opened_count,
            sum(unsubscribed_count) as unsubscribed_count
        from `{{ bigquery_raw_dataset }}.brevo_transactional`
        group by tag, template, user_id
    ),

    infos_aggregated as (
        select
            tag,
            template,
            sum(delivered_count) as delivered_count,
            sum(opened_count) as opened_count,
            sum(case when opened_count > 0 then 1 else 0 end) as unique_opened_count,
            sum(unsubscribed_count) as unsubscribed_count
        from user_infos
        group by tag, template
    )

select
    tag,
    template,
    delivered_count,
    opened_count,
    unique_opened_count,
    unsubscribed_count,
    date('{{ ds }}') as update_date
from infos_aggregated
