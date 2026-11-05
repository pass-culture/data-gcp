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
    Collective bookings split by educational domain (school geography), rolled
    up from metrics_booking__collective_by_domain. Monthly flows, only for the
    months with at least one booking. An offer with several domains counts in
    each of them, so domains are not summable.
#}
{% set kpis = {
    "total_reservation": [
        "sum(total_collective_bookings)",
        "1",
    ],
    "total_montant_depense": [
        "sum(total_collective_booking_amount)",
        "1",
    ],
    "total_tickets_generes": [
        "sum(total_collective_tickets)",
        "1",
    ],
    "total_eple_impliquees": [
        "sum(total_collective_institutions)",
        "1",
    ],
    "pct_reservations_labelisees": [
        "nullif(sum(total_labelled_collective_bookings), 0)",
        "sum(total_collective_bookings)",
    ],
} %}

select
    partition_month,
    timestamp('{{ ts() }}') as updated_at,
    dimension_name,
    dimension_value,
    concat(kpi_base_name, '_', domain_name) as kpi_name,
    numerator,
    denominator,
    safe_divide(numerator, denominator) as kpi
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
                case
                    educational_domain_name
                    {% for domain in get_domains() %}
                        when '{{ domain.label }}' then '{{ domain.name }}'
                    {% endfor %}
                end as domain_name,
                {% for kpi_name, exprs in kpis.items() %}
                    cast({{ exprs[0] }} as numeric) as {{ kpi_name }}_numerator,
                    cast({{ exprs[1] }} as int64) as {{ kpi_name }}_denominator
                    {% if not loop.last %},{% endif %}
                {% endfor %}
            from {{ ref("metrics_booking__collective_by_domain") }}
            {% if is_incremental() %}
                where
                    partition_month
                    = date_trunc(date_sub(date('{{ ds() }}'), interval 1 month), month)
            {% endif %}
            group by partition_month, dimension_name, dimension_value, domain_name
            having domain_name is not null and sum(total_collective_bookings) > 0
        {% endfor %}
    ) unpivot (
        (numerator, denominator) for kpi_base_name in (
            {% for kpi_name in kpis.keys() %}
                (
                    {{ kpi_name }}_numerator, {{ kpi_name }}_denominator
                ) as '{{ kpi_name }}'
                {% if not loop.last %},{% endif %}
            {% endfor %}
        )
    )
