{% macro generate_metric_by_dimensions(
    base_cte, kpi_name, numerator_expr, denominator_expr, dimensions
) %}
    {#
        Generate KPI metrics across multiple dimensions using UNION ALL.

        Parameters:
            base_cte: Name of the CTE to select from
            kpi_name: Name of the KPI metric
            numerator_expr: SQL expression for the numerator
            denominator_expr: SQL expression for the denominator
            dimensions: Array of dimension objects from get_dimensions()

        Returns: SQL with UNION ALL of metric calculations for each dimension
    #}
    {% for dim in dimensions %}
        {% if not loop.first %}
            union all
        {% endif %}
        {{ metric_select(base_cte, kpi_name, numerator_expr, denominator_expr, dim) }}
    {% endfor %}
{% endmacro %}
