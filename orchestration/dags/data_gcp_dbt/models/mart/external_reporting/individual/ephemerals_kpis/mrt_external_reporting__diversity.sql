-- noqa: disable=all
{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = [
    {"name": "NAT", "value_expr": "'NAT'"},
    {"name": "REG", "value_expr": "user_region_name"},
    {"name": "DEP", "value_expr": "user_department_name"},
    {"name": "EPCI", "value_expr": "user_epci"},
    {"name": "COM", "value_expr": "user_city"},
] %}

{% set categories = [
    {"name": "SPECTACLE", "value": "spectacle_vivant"},
    {"name": "MUSEE", "value": "musee"},
    {"name": "CINEMA", "value": "cinema"},
    {"name": "PRATIQUE_ART", "value": "pratique_artistique"},
    {"name": "INSTRUMENT", "value": "instrument"},
    {"name": "LIVRE", "value": "livre"},
    {"name": "MUSIQUE_LIVE", "value": "concert"},
] %}

{% for category in categories %}
    {% if loop.first %}
        with
    {% endif %}
        booked_category_{{ category.value }} as (
            select distinct user_id
            from {{ ref("mrt_global__booking") }}
            where booking_is_used and offer_category_id = "{{ category.name }}"
        )
        {% if not loop.last %}, {% endif %}
{% endfor %}

{% for category in categories %}
    {% for dim in dimensions %}
        {% if not loop.first %}
            union all
        {% endif %}
        select
            date_trunc(date(last_deposit_expiration_date), month) as partition_month,
            timestamp("{{ ts() }}") as updated_at,
            '{{ dim.name }}' as dimension_name,
            {{ dim.value_expr }} as dimension_value,
            "pct_beneficiaires_ayant_reserve_dans_la_categorie_{{ category.value }}"
            as kpi_name,
            coalesce(count(distinct bc.user_id), 0) as numerator,
            coalesce(count(distinct user_id), 0) as denominator,
            coalesce(
                safe_divide(count(distinct bc.user_id), count(distinct user_id)), 0
            ) as kpi
        from {{ ref("mrt_global__user") }}
        left join booked_category_{{ category.value }} as bc using (user_id)
        where
            total_deposit_amount >= 300
            {% if is_incremental() %}
                and date_trunc(last_deposit_expiration_date, month)
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
        group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
    {% endfor %}
    {% if not loop.last %}
        union all
    {% endif %}
{% endfor %}

{% for dim in dimensions %}
    union all
    select
        date_trunc(date(user_expiration_month), month) as partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        "pct_beneficiaires_ayant_reserve_dans_3_categories" as kpi_name,
        coalesce(sum(total_3_category_booked_users), 0) as numerator,
        coalesce(sum(total_users), 0) as denominator,
        coalesce(
            safe_divide(sum(total_3_category_booked_users), sum(total_users)), 0
        ) as kpi
    from {{ ref("mrt_native__outgoing_cohort") }}
    {% if is_incremental() %}
        where
            date_trunc(user_expiration_month, month)
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
    {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
{% endfor %}
