with
    last_year_beginning_date as (
        select educational_year_beginning_date as last_year_start_date
        from `{{ bigquery_raw_dataset }}.applicative_database_educational_year`
        where
            educational_year_beginning_date <= date_sub(date("{{ ds }}"), interval 1 year)
            and educational_year_expiration_date
            > date_sub(date("{{ ds }}"), interval 1 year)
    ),
    last_day as (
        select
            date_trunc(date, month) as date,
            max(date) as last_date,
            max(cast(adage_id as int)) as last_adage_id
        from `{{ bigquery_analytics_dataset }}.adage_involved_institution`
        where
            date <= date("{{ ds }}")
            and date > (select last_year_start_date from last_year_beginning_date)
        group by 1
    )

select
    date_trunc(involved.date, month) as month,
    "{{ params.group_type }}" as dimension_name,
    {% if params.group_type == "NAT" %} 'NAT'
    {% else %} {{ params.group_type_name }}
    {% endif %} as dimension_value,
    null as user_type,  -- nous n'avons pas le détail par age dans la table adage_involved_institution
    "taux_participation_eac_ecoles" as indicator,
    sum(institutions) as numerator,  -- active_institutions
    sum(total_institutions) as denominator  -- total_institutions
from `{{ bigquery_analytics_dataset }}.adage_involved_institution` as involved
-- take only last day for each month.
join
    last_day
    on last_day.last_date = involved.date
    and date_trunc(involved.date, month) = last_day.date
    and last_day.last_adage_id = cast(involved.adage_id as int)
left join
    `{{ bigquery_seed_dataset }}.region_department` as rd
    on involved.department_code = rd.num_dep
where
    {% if params.group_type == "NAT" %} department_code = '-1'
    {% else %} not department_code = '-1'
    {% endif %}
group by 1, 2, 3, 4, 5
