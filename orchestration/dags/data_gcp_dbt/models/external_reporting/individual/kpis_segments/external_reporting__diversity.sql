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
    geography) who booked in 3 categories or more, and in each category.

    Counts are protected with the cell key method (see statistical_confidentiality
    doc):
        - NAT, REG and DEP are sums of the perturbed cells of exp_base__diversity
          and exp_base__diversity_by_category, so that they match the vidoc and
          data.gouv.fr exports;
        - EPCI and COM are rolled up from metrics_diversity and
          metrics_diversity__by_category with their additive cell keys, then
          perturbed here.
    Sub-cohorts (3+ categories, category bookers) are capped at the expired count.
#}
{% set base_dimensions = ["NAT", "REG", "DEP"] %}
{% set categories = get_categories() %}
{% set dimensions = [
    {"name": "NAT", "value_expr": "'NAT'"},
    {"name": "REG", "value_expr": "region_name"},
    {"name": "DEP", "value_expr": "department_name"},
    {"name": "EPCI", "value_expr": "epci_code"},
    {"name": "COM", "value_expr": "municipality_code"},
] %}

{% set month_filter %}
    {% if is_incremental() %}
        and deposit_expiration_month
        = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
    {% endif %}
{% endset %}

with
    expired_cells as (
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            {% if dim.name in base_dimensions %}
                select
                    deposit_expiration_month as partition_month,
                    '{{ dim.name }}' as dimension_name,
                    {{ dim.value_expr }} as dimension_value,
                    sum(total_expired_deposit_beneficiaries) as total_expired,
                    sum(total_3plus_category_booked_beneficiaries) as total_3plus,
                    cast(null as int64) as cell_key_expired,
                    cast(null as int64) as cell_key_3plus
                from {{ ref("exp_base__diversity") }}
            {% else %}
                select
                    deposit_expiration_month as partition_month,
                    '{{ dim.name }}' as dimension_name,
                    {{ dim.value_expr }} as dimension_value,
                    sum(total_expired_deposit_beneficiaries) as total_expired,
                    sum(total_3plus_category_booked_beneficiaries) as total_3plus,
                    mod(sum(cell_key_expired), 256) as cell_key_expired,
                    mod(sum(cell_key_3plus), 256) as cell_key_3plus
                from {{ ref("metrics_diversity") }}
            {% endif %}
            where {{ dim.value_expr }} is not null {{ month_filter }}
            group by partition_month, dimension_name, dimension_value
        {% endfor %}
    ),

    booked_cells as (
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                deposit_expiration_month as partition_month,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                offer_category_id,
                sum(total_category_booked_beneficiaries) as total_booked,
            {% if dim.name in base_dimensions %}
                    cast(null as int64) as cell_key_category
                from {{ ref("exp_base__diversity_by_category") }}
            {% else %}
                    mod(sum(cell_key_category), 256) as cell_key_category
                from {{ ref("metrics_diversity__by_category") }}
            {% endif %}
            where {{ dim.value_expr }} is not null {{ month_filter }}
            group by partition_month, dimension_name, dimension_value, offer_category_id
        {% endfor %}
    ),

    -- perturb the cells that carry a cell key (EPCI, COM); base cells are
    -- already perturbed
    expired as (
        select
            e.partition_month,
            e.dimension_name,
            e.dimension_value,
            if(
                e.cell_key_expired is null,
                e.total_expired,
                greatest(e.total_expired + coalesce(pt_expired.perturbation, 0), 0)
            ) as total_expired,
            if(
                e.cell_key_3plus is null,
                e.total_3plus,
                greatest(e.total_3plus + coalesce(pt_3plus.perturbation, 0), 0)
            ) as total_3plus
        from expired_cells as e
        left join
            {{ ref("perturbation_table__individual") }} as pt_expired
            on e.total_expired between pt_expired.count_min and pt_expired.count_max
            and e.cell_key_expired
            between pt_expired.cell_key_min and pt_expired.cell_key_max
        left join
            {{ ref("perturbation_table__individual") }} as pt_3plus
            on e.total_3plus between pt_3plus.count_min and pt_3plus.count_max
            and e.cell_key_3plus between pt_3plus.cell_key_min and pt_3plus.cell_key_max
    ),

    booked as (
        select
            b.partition_month,
            b.dimension_name,
            b.dimension_value,
            b.offer_category_id,
            if(
                b.cell_key_category is null,
                b.total_booked,
                greatest(b.total_booked + coalesce(pt.perturbation, 0), 0)
            ) as total_booked
        from booked_cells as b
        left join
            {{ ref("perturbation_table__individual") }} as pt
            on b.total_booked between pt.count_min and pt.count_max
            and b.cell_key_category between pt.cell_key_min and pt.cell_key_max
    )

-- sub-cohorts are capped at the perturbed expired count (shares <= 100%)
select
    partition_month,
    timestamp("{{ ts() }}") as updated_at,
    dimension_name,
    dimension_value,
    "pct_beneficiaires_ayant_reserve_dans_3_categories" as kpi_name,
    coalesce(least(total_3plus, total_expired), 0) as numerator,
    coalesce(total_expired, 0) as denominator,
    coalesce(safe_divide(least(total_3plus, total_expired), total_expired), 0) as kpi
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
    coalesce(least(booked.total_booked, expired.total_expired), 0) as numerator,
    coalesce(expired.total_expired, 0) as denominator,
    coalesce(
        safe_divide(
            least(booked.total_booked, expired.total_expired), expired.total_expired
        ),
        0
    ) as kpi
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
