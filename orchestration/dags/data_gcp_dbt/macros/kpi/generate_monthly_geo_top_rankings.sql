{% macro generate_monthly_geo_top_rankings(
    base_cte,
    entity_fields,
    aggregated_metrics,
    ranking_metric,
    rank_column_name,
    additional_partition_by=[],
    top_n=50
) %}
    {% set geo_levels = [
        {"level": "national", "value_expr": "'NAT'", "group_expr": "'NAT'"},
        {
            "level": "region",
            "value_expr": "venue_region_name",
            "group_expr": "venue_region_name",
        },
        {
            "level": "department",
            "value_expr": "venue_department_name",
            "group_expr": "venue_department_name",
        },
    ] %}

    {% for geo in geo_levels %}
        {% if not loop.first %}
            union all
        {% endif %}

        select
            partition_month,
            '{{ geo.level }}' as geo_level,
            cast({{ geo.value_expr }} as string) as geo_value,
            {% for field in entity_fields %} {{ field }}, {% endfor %}
            {% for metric in aggregated_metrics %}
                sum({{ metric.field }}) as {{ metric.alias }},
            {% endfor %}
            row_number() over (
                partition by
                    partition_month,
                    {% for partition_field in additional_partition_by %}
                        {{ partition_field }},
                    {% endfor %}
                    {{ geo.group_expr }}
                -- entity fields break ties so that the rank (and the top_n cut)
                -- is deterministic from one run to the next
                order by
                    sum({{ ranking_metric }}) desc
                    {% for field in entity_fields %}, {{ field }} {% endfor %}
            ) as {{ rank_column_name }}
        from {{ base_cte }}
        where {{ geo.value_expr }} is not null
        group by
            partition_month,
            geo_level,
            geo_value
            {% for field in entity_fields %}, {{ field }} {% endfor %}
        qualify {{ rank_column_name }} <= {{ top_n }}
    {% endfor %}
{% endmacro %}
