{% macro metric_select(base_cte, kpi_name, numerator_expr, denominator_expr, dim) %}
    {#
        Helper macro to generate a single metric select statement for one dimension.
        Used internally by generate_metric_by_dimensions.

        Parameters:
            base_cte: Name of the CTE to select from
            kpi_name: Name of the KPI metric
            numerator_expr: SQL expression for the numerator
            denominator_expr: SQL expression for the denominator
            dim: Dimension object with 'name' and 'value_expr' keys
    #}
    select
        partition_month,
        updated_at,
        '{{ dim.name }}' as dimension_name,
        {{ dim.value_expr }} as dimension_value,
        "{{ kpi_name }}" as kpi_name,
        {{ numerator_expr }} as numerator,
        {{ denominator_expr }} as denominator
    from {{ base_cte }}
    group by partition_month, updated_at, dimension_name, dimension_value, kpi_name
{% endmacro %}
