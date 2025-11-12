{% macro generate_coverage_metrics_by_age(age_groups, dimensions) %}
    {#
        Generate coverage rate metrics by age group across dimensions.

        Calculates the coverage rate (taux_couverture) for each age group,
        representing the proportion of the population that are pass Culture beneficiaries.

        Parameters:
            age_groups: Array of ages from get_age_groups() (e.g., [15, 16, 17, 18])
            dimensions: Array of dimension objects from get_dimensions()

        Returns: SQL with UNION ALL of coverage metrics for each age and dimension

        Note: Uses population_decimal_age which is age + 1 in the source data
    #}
    {% for dim in dimensions %}
        {% if not loop.first %}
            union all
        {% endif %}
        {% for age in age_groups|reverse %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                date_trunc(population_snapshot_month, month) as partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                "taux_couverture_{{ age }}" as kpi_name,
                sum(total_users_last_12_months) as numerator,
                sum(total_population_last_12_months) as denominator,
                safe_divide(
                    sum(total_users_last_12_months),
                    sum(total_population_last_12_months)
                ) as kpi
            from {{ ref("mrt_native__monthly_beneficiary_coverage") }}
            where
                population_decimal_age = "{{ age + 1 }}"
                and population_snapshot_month <= date_trunc(current_date, month)
                {% if is_incremental() %}
                    and date_trunc(population_snapshot_month, month)
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
        {% endfor %}
    {% endfor %}
{% endmacro %}
