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
    Collective bookings split by offer format (school geography), rolled up from
    metrics_booking__collective_by_format. Monthly flows. An offer with several
    formats counts in each of them, so formats are not summable.
#}
{% set kpis = {
    "total_reservations_format": "total_collective_bookings",
    "total_montant_depense_format": "total_collective_booking_amount",
    "total_tickets_generes_format": "total_collective_tickets",
} %}

select
    partition_month,
    timestamp('{{ ts() }}') as updated_at,
    dimension_name,
    dimension_value,
    concat(kpi_base_name, '_', lower(collective_offer_format)) as kpi_name,
    numerator,
    1 as denominator,
    numerator as kpi
from
    (
        {% for dim in get_dimensions("institution", "academic_extended") %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                collective_offer_format,
                {% for kpi_name, column in kpis.items() %}
                    cast(sum({{ column }}) as float64) as {{ kpi_name }}
                    {% if not loop.last %},{% endif %}
                {% endfor %}
            from {{ ref("metrics_booking__collective_by_format") }}
            where
                collective_offer_format is not null
                {% if is_incremental() %}
                    and partition_month
                    = date_trunc(date_sub(date('{{ ds() }}'), interval 1 month), month)
                {% endif %}
            group by
                partition_month,
                dimension_name,
                dimension_value,
                collective_offer_format
        {% endfor %}
    )
    unpivot (numerator for kpi_base_name in ({{ kpis.keys() | join(", ") }}))
where dimension_value is not null
