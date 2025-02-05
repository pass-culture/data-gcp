with
    last_day_of_month as (
        select date_trunc(day, month) as month, max(day) as last_active_date
        from `{{ bigquery_analytics_dataset }}.retention_partner_history`
        group by 1
    ),

    all_partners as (
        select
            date_trunc(day, month) as month,
            "{{ params.group_type }}" as dimension_name,
            {% if params.group_type == "NAT" %} 'NAT'
            {% else %} {{ params.group_type_name }}
            {% endif %} as dimension_value,
            count(
                distinct if(
                    date_diff(day, last_bookable_date, day) <= 365,
                    retention_partner_history.partner_id,
                    null
                )
            ) as numerator,
            count(
                distinct if(
                    retention_partner_history.first_offer_creation_date <= day,
                    retention_partner_history.partner_id,
                    null
                )
            ) as denominator,
        from
            `{{ bigquery_analytics_dataset }}.retention_partner_history` retention_partner_history
        inner join
            last_day_of_month ldm
            on ldm.last_active_date = retention_partner_history.day
        left join
            `{{ bigquery_analytics_dataset }}.global_cultural_partner` global_cultural_partner
            on retention_partner_history.partner_id = global_cultural_partner.partner_id
        left join
            `{{ bigquery_seed_dataset }}.region_department` region_department
            on global_cultural_partner.partner_department_code
            = region_department.num_dep
        group by 1, 2, 3
    )
select
    month,
    dimension_name,
    dimension_value,
    null as user_type,
    "taux_retention_partenaires" as indicator,
    denominator,
    numerator
from all_partners
where month >= "2023-01-01"
