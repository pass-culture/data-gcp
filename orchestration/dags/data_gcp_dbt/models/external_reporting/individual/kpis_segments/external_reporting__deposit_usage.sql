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
#}
{% set kpis = {
    "montant_moyen_octroye_a_l_expiration_du_credit": "total_deposit_amount_at_expiration",
    "montant_moyen_depense_a_l_expiration_du_credit": "total_actual_amount_spent_at_expiration",
} %}
{% set dimensions = [
    {"name": "NAT", "value_expr": "'NAT'"},
    {"name": "REG", "value_expr": "region_name"},
    {"name": "DEP", "value_expr": "department_name"},
    {"name": "EPCI", "value_expr": "epci_code"},
    {"name": "COM", "value_expr": "municipality_code"},
] %}

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
                    and deposit_expiration_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by partition_month, dimension_name, dimension_value
        {% endfor %}
    )
    unpivot (numerator for kpi_name in ({{ kpis.keys() | join(", ") }}))
