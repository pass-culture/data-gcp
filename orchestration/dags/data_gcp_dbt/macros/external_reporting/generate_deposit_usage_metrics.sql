{% macro generate_deposit_usage_metrics(kpis, dimensions) %}
    {#
        Generate deposit usage metrics across dimensions.

        Calculates average amounts (granted and spent) per user at deposit expiration.

        Parameters:
            kpis: Array of KPI objects with 'name' and 'numerator_field' keys
            dimensions: Array of dimension objects from get_dimensions()

        Returns: SQL with UNION ALL of deposit usage metrics for each KPI and dimension

        Example KPIs structure:
            [
                {
                    "name": "montant_moyen_octroye_a_l_expiration_du_credit",
                    "numerator_field": "total_deposit_amount"
                },
                {
                    "name": "montant_moyen_depense_a_l_expiration_du_credit",
                    "numerator_field": "total_actual_amount_spent"
                }
            ]
    #}
    {% for kpi in kpis %}
        {% if not loop.first %}
            union all
        {% endif %}
        {% for dim in dimensions %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                date_trunc(
                    date(last_deposit_expiration_date), month
                ) as partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                "{{ kpi.name }}" as kpi_name,
                count(distinct user_id) as denominator,
                sum({{ kpi.numerator_field }}) as numerator,
                safe_divide(
                    sum({{ kpi.numerator_field }}), count(distinct user_id)
                ) as kpi
            from {{ ref("mrt_global__user") }}
            {% if is_incremental() %}
                where
                    date_trunc(last_deposit_expiration_date, month)
                    = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
            {% endif %}
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
        {% endfor %}
    {% endfor %}
{% endmacro %}
