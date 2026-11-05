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
    Collective bookings received by the cultural partners of the territory
    (venue geography), so that cultural partner pages read individual and
    collective figures from the same table. Same metrics as the venue side KPIs
    of external_reporting__eac_offer_booking. Monthly flows.
#}
{% set kpis = {
    "total_reservations_collectives": "total_collective_bookings",
    "total_montant_engage": "total_collective_amount_spent",
} %}

select
    partition_month,
    timestamp('{{ ts() }}') as updated_at,
    dimension_name,
    dimension_value,
    kpi_name,
    numerator,
    1 as denominator,
    numerator as kpi
from
    (
        {% for dim in get_dimensions("venue", "geo_full") %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                {% for kpi_name, column in kpis.items() %}
                    cast(sum({{ column }}) as float64) as {{ kpi_name }}
                    {% if not loop.last %},{% endif %}
                {% endfor %}
            from {{ ref("metrics_booking__collective") }}
            where
                1 = 1
                {% if is_incremental() %}
                    and partition_month
                    = date_trunc(date_sub(date('{{ ds() }}'), interval 1 month), month)
                {% endif %}
            group by partition_month, dimension_name, dimension_value
        {% endfor %}
    )
    unpivot (numerator for kpi_name in ({{ kpis.keys() | join(", ") }}))
