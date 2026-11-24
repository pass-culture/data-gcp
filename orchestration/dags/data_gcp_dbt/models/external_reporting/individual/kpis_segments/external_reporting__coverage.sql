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
    Coverage rate by age over the last 12 months: beneficiaries with an active
    credit at the exact age (milestone_age) over the population of that age,
    both counted on the population department. taux_couverture_<age> reads
    milestone_age <age> + 1, as the source data counts age + 1.
    Beneficiary counts are the perturbed cells (department x milestone age) of
    exp_base__beneficiary_coverage: REG and NAT are sums of these cells, so that
    they match the vidoc and data.gouv.fr exports. Population counts are public
    INSEE estimates and are not perturbed.
#}
{% set age_groups = get_age_groups() %}
{% set dimensions = [
    {"name": "NAT", "value_expr": "'NAT'"},
    {"name": "REG", "value_expr": "region_name"},
    {"name": "DEP", "value_expr": "department_name"},
] %}

with
    coverage as (
        select
            population.partition_month,
            population.region_name,
            population.department_name,
            population.total_population_last_12_months,
            cast(population.milestone_age as int64) - 1 as age,
            coalesce(
                beneficiary.total_beneficiaries_last_12_months, 0
            ) as total_beneficiaries_last_12_months
        from {{ ref("metrics_population__coverage") }} as population
        left join
            {{ ref("exp_base__beneficiary_coverage") }} as beneficiary
            on population.partition_month = beneficiary.partition_month
            and cast(population.milestone_age as int64) = beneficiary.milestone_age
            and population.department_code = beneficiary.department_code
        where
            population.milestone_age in (
                {% for age in age_groups %}
                    "{{ age + 1 }}"{% if not loop.last %},{% endif %}
                {% endfor %}
            )
            and population.partition_month <= date_trunc(current_date, month)
            {% if is_incremental() %}
                and population.partition_month
                = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
    )

{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
    select
        partition_month,
        timestamp("{{ ts() }}") as updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        concat("taux_couverture_", age) as kpi_name,
        sum(total_beneficiaries_last_12_months) as numerator,
        sum(total_population_last_12_months) as denominator,
        safe_divide(
            sum(total_beneficiaries_last_12_months),
            sum(total_population_last_12_months)
        ) as kpi
    from coverage
    group by partition_month, dimension_name, dimension_value, age
{% endfor %}
