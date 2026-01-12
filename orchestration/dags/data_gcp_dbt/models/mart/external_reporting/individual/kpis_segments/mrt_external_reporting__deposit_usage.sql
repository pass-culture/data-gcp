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

{% set dimensions = get_dimensions("user", "geo_full") %}

{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        date_trunc(date(last_deposit_expiration_date), month) as partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        "montant_moyen_octroye_a_l_expiration_du_credit" as kpi_name,
        coalesce(sum(total_deposit_amount), 0) as numerator,
        coalesce(count(distinct user_id), 0) as denominator,
        coalesce(
            safe_divide(sum(total_deposit_amount), count(distinct user_id)), 0
        ) as kpi
    from {{ ref("mrt_global__user_beneficiary") }}
    where
        current_deposit_type in ("GRANT_18", "GRANT_17_18")
        {% if is_incremental() %}
            and date_trunc(last_deposit_expiration_date, month)
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
{% endfor %}
union all
{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        date_trunc(date(last_deposit_expiration_date), month) as partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        "montant_moyen_depense_a_l_expiration_du_credit" as kpi_name,
        coalesce(sum(total_actual_amount_spent), 0) as numerator,
        coalesce(count(distinct user_id), 0) as denominator,
        coalesce(
            safe_divide(sum(total_actual_amount_spent), count(distinct user_id)), 0
        ) as kpi
    from {{ ref("mrt_global__user_beneficiary") }}
    where
        current_deposit_type in ("GRANT_18", "GRANT_17_18")
        {% if is_incremental() %}
            and date_trunc(last_deposit_expiration_date, month)
            = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
        {% endif %}
    group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
{% endfor %}
