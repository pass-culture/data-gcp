{% for kpi_details in params.kpis_list %}
    {% for granularity in ["region", "departement", "academie", "all"] %}
        select
            safe_cast(month as date) as month,
            true as is_current_year,
            safe_cast(dimension_name as string) as dimension_name,
            safe_cast(dimension_value as string) as dimension_value,
            safe_cast(user_type as string) as user_type,
            safe_cast(indicator as string) as indicator,
            safe_cast(numerator as integer) as numerator,
            safe_cast(denominator as integer) as denominator
        from
            `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(ds) }}_{{ kpi_details.table_name }}_{{ granularity }}`

        union all

        select

            date(
                date_trunc(date_add(safe_cast(month as date), interval 1 year), month)
            ) as month,
            false as is_current_year,
            safe_cast(dimension_name as string) as dimension_name,
            safe_cast(dimension_value as string) as dimension_value,
            safe_cast(user_type as string) as user_type,
            safe_cast(indicator as string) as indicator,
            safe_cast(numerator as integer) as numerator,
            safe_cast(denominator as integer) as denominator
        from
            `{{ bigquery_tmp_dataset }}.{{ yyyymmdd(ds) }}_{{ kpi_details.table_name }}_{{ granularity }}`
        where
            date_trunc(date_sub(current_date, interval 1 year), month)
            >= date_trunc(safe_cast(month as date), month)
        {% if not loop.last %}
            union all
        {% endif %}

    {% endfor %}

    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}
