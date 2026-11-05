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
    Share of the beneficiaries whose credit expired in the month (user
    geography) who booked in 3 categories or more, and in each category,
    rolled up from metrics_diversity and metrics_diversity__by_category.
#}
{% set categories = get_categories() %}
{% set dimensions = [
    {"name": "NAT", "value_expr": "'NAT'"},
    {"name": "REG", "value_expr": "region_name"},
    {"name": "DEP", "value_expr": "department_name"},
    {"name": "EPCI", "value_expr": "epci_code"},
    {"name": "COM", "value_expr": "municipality_code"},
] %}

with
    expired as (
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                deposit_expiration_month as partition_month,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                sum(total_expired_deposit_beneficiaries) as total_expired,
                sum(total_3plus_category_booked_beneficiaries) as total_3plus
            from {{ ref("metrics_diversity") }}
            where
                {{ dim.value_expr }} is not null
                {% if is_incremental() %}
                    and deposit_expiration_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by partition_month, dimension_name, dimension_value
        {% endfor %}
    ),

    booked as (
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                deposit_expiration_month as partition_month,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                offer_category_id,
                sum(total_category_booked_beneficiaries) as total_booked
            from {{ ref("metrics_diversity__by_category") }}
            where
                {{ dim.value_expr }} is not null
                {% if is_incremental() %}
                    and deposit_expiration_month
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by partition_month, dimension_name, dimension_value, offer_category_id
        {% endfor %}
    )

select
    partition_month,
    timestamp("{{ ts() }}") as updated_at,
    dimension_name,
    dimension_value,
    "pct_beneficiaires_ayant_reserve_dans_3_categories" as kpi_name,
    coalesce(total_3plus, 0) as numerator,
    coalesce(total_expired, 0) as denominator,
    coalesce(safe_divide(total_3plus, total_expired), 0) as kpi
from expired
union all
-- one row per category, 0 when no expired beneficiary booked in it
select
    expired.partition_month,
    timestamp("{{ ts() }}") as updated_at,
    expired.dimension_name,
    expired.dimension_value,
    concat(
        "pct_beneficiaires_ayant_reserve_dans_la_categorie_", category.label
    ) as kpi_name,
    coalesce(booked.total_booked, 0) as numerator,
    coalesce(expired.total_expired, 0) as denominator,
    coalesce(safe_divide(booked.total_booked, expired.total_expired), 0) as kpi
from expired
cross join
    unnest(
        [
            {% for category in categories %}
                struct(
                    '{{ category.name }}' as id, '{{ category.value_expr }}' as label
                )
                {% if not loop.last %},{% endif %}
            {% endfor %}
        ]
    ) as category
left join
    booked
    on expired.partition_month = booked.partition_month
    and expired.dimension_name = booked.dimension_name
    and expired.dimension_value = booked.dimension_value
    and category.id = booked.offer_category_id
