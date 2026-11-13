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

{#
    Average credit granted and spent by the beneficiaries whose credit expired
    in the month (user geography), rolled up from metrics_beneficiary__expired_cohorts.

    The *_trimestre KPIs are the same averages over the calendar quarter, on the
    row of its last month only. Monthly cohorts of small territories often
    count fewer than 5 beneficiaries (masked), quarterly ones rarely. Quarters do
    not overlap, so that a masked month cannot be deduced by difference.
#}
{% set kpis = {
    "montant_moyen_octroye_a_l_expiration_du_credit": "total_deposit_amount_at_expiration",
    "montant_moyen_depense_a_l_expiration_du_credit": "total_actual_amount_spent_at_expiration",
} %}
{% set quarterly_kpis = {
    "montant_moyen_octroye_a_l_expiration_du_credit_trimestre": "total_deposit_amount_at_expiration",
    "montant_moyen_depense_a_l_expiration_du_credit_trimestre": "total_actual_amount_spent_at_expiration",
} %}
{% set last_month = (
    'date_trunc(date_sub(date("' ~ ds() ~ '"), interval 1 month), month)'
) %}
{% set dimensions = [
    {"name": "NAT", "value_expr": "'NAT'"},
    {"name": "REG", "value_expr": "region_name"},
    {"name": "DEP", "value_expr": "department_name"},
    {"name": "EPCI", "value_expr": "epci_code"},
    {"name": "COM", "value_expr": "municipality_code"},
] %}

with
    monthly as (
        select
            partition_month,
            dimension_name,
            dimension_value,
            kpi_name,
            numerator,
            denominator
        from
            (
                {% for dim in dimensions %}
                    {% if not loop.first %}
                        union all
                    {% endif %}
                    select
                        deposit_expiration_month as partition_month,
                        '{{ dim.name }}' as dimension_name,
                        {{ dim.value_expr }} as dimension_value,
                        {% for kpi_name, column in kpis.items() %}
                            sum({{ column }}) as {{ kpi_name }},
                        {% endfor %}
                        sum(total_expired_deposit_beneficiaries) as denominator
                    from {{ ref("metrics_beneficiary__expired_cohorts") }}
                    where
                        {{ dim.value_expr }} is not null
                        {% if is_incremental() %}
                            and deposit_expiration_month = {{ last_month }}
                        {% endif %}
                    group by partition_month, dimension_name, dimension_value
                {% endfor %}
            )
            unpivot (numerator for kpi_name in ({{ kpis.keys() | join(", ") }}))
    ),

    -- calendar quarters, on the row of their last month
    quarterly as (
        select
            partition_month,
            dimension_name,
            dimension_value,
            kpi_name,
            numerator,
            denominator
        from
            (
                {% for dim in dimensions %}
                    {% if not loop.first %}
                        union all
                    {% endif %}
                    select
                        last_day(deposit_expiration_month, quarter) as quarter_end,
                        date_trunc(
                            last_day(deposit_expiration_month, quarter), month
                        ) as partition_month,
                        '{{ dim.name }}' as dimension_name,
                        {{ dim.value_expr }} as dimension_value,
                        {% for kpi_name, column in quarterly_kpis.items() %}
                            sum({{ column }}) as {{ kpi_name }},
                        {% endfor %}
                        sum(total_expired_deposit_beneficiaries) as denominator
                    from {{ ref("metrics_beneficiary__expired_cohorts") }}
                    where
                        {{ dim.value_expr }} is not null
                        -- complete quarters only
                        and date_trunc(
                            last_day(deposit_expiration_month, quarter), month
                        )
                        <= {{ last_month }}
                        {% if is_incremental() %}
                            and date_trunc(
                                last_day(deposit_expiration_month, quarter), month
                            )
                            = {{ last_month }}
                        {% endif %}
                    group by
                        quarter_end, partition_month, dimension_name, dimension_value
                {% endfor %}
            ) unpivot (
                numerator for kpi_name in ({{ quarterly_kpis.keys() | join(", ") }})
            )
    )

select
    partition_month,
    timestamp("{{ ts() }}") as updated_at,
    dimension_name,
    dimension_value,
    kpi_name,
    numerator,
    denominator,
    coalesce(safe_divide(numerator, denominator), 0) as kpi
from
    (
        select *
        from monthly
        union all
        select *
        from quarterly
    )
