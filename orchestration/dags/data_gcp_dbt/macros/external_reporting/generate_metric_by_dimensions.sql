{% macro metric_select(base_cte, kpi_name, numerator_expr, denominator_expr, dim) %}
select
    partition_month,
    updated_at,
    '{{ dim.name }}' as dimension_name,
    {{ dim.value_expr }} as dimension_value,
    "{{ kpi_name }}" as kpi_name,
    {{ numerator_expr }} as numerator,
    {{ denominator_expr }} as denominator
from {{ base_cte }}
group by
    partition_month, updated_at, dimension_name, dimension_value, kpi_name
{% endmacro %}

{% macro generate_metric_by_dimensions(base_cte, kpi_name, numerator_expr, denominator_expr, dimensions) %}
    {% for dim in dimensions %}
        {% if not loop.first %}
            union all
        {% endif %}
        {{ metric_select(base_cte, kpi_name, numerator_expr, denominator_expr, dim) }}
    {% endfor %}
{% endmacro %}

{% macro generate_activity_metrics(base_cte, activity_list, dimensions) %}
    {% for activity in activity_list %}
        {% if not loop.first %}
            union all
        {% endif %}
        {{ generate_metric_by_dimensions(
            base_cte,
            'pct_beneficiaire_actuel_' ~ activity.value,
            'count(distinct case when user_activity = "' ~ activity.activity ~ '" then user_id end)',
            'count(distinct user_id)',
            dimensions
        ) }}
    {% endfor %}
{% endmacro %}

{% macro generate_coverage_metrics_by_age(age_groups, dimensions) %}
    {% for age in age_groups %}
        {% if not loop.first %}
            union all
        {% endif %}
        {% for dim in dimensions %}
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
                    sum(total_users_last_12_months), sum(total_population_last_12_months)
                ) as kpi
            from {{ ref("mrt_native__monthly_beneficiary_coverage") }}
            where
                population_decimal_age = "{{ age + 1 }}"
                and population_snapshot_month <= date_trunc(current_date, month)
                {% if is_incremental() %}
                    and date_trunc(population_snapshot_month, month)
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
                {% endif %}
            group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
        {% endfor %}
    {% endfor %}
{% endmacro %}

{% macro generate_deposit_usage_metrics(kpis, dimensions) %}
    {% for kpi in kpis %}
        {% if not loop.first %}
            union all
        {% endif %}
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                date_trunc(date(last_deposit_expiration_date), month) as partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                "{{ kpi.name }}" as kpi_name,
                count(distinct user_id) as denominator,
                sum({{ kpi.numerator_field }}) as numerator,
                safe_divide(sum({{ kpi.numerator_field }}), count(distinct user_id)) as kpi
            from {{ ref("mrt_global__user") }}
            {% if is_incremental() %}
                where
                    date_trunc(last_deposit_expiration_date, month)
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
            group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
        {% endfor %}
    {% endfor %}
{% endmacro %}
