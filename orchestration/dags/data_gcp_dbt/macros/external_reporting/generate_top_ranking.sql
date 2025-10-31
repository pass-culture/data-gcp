{% macro generate_top_ranking_by_dimensions(
    base_cte,
    dimensions,
    entity_fields,
    aggregated_metrics,
    ranking_metric,
    additional_partition_by=[],
    top_n=50
) %}
    {% for dim in dimensions %}
        {% if not loop.first %}
            union all
        {% endif %}
        select
            partition_month,
            updated_at,
            '{{ dim.name }}' as dimension_name,
            {{ dim.value_expr }} as dimension_value,
            {% for field in entity_fields %}
                {{ field }},
            {% endfor %}
            {% for metric in aggregated_metrics %}
                sum({{ metric.field }}) as {{ metric.alias }},
            {% endfor %}
            row_number() over (
                partition by
                    partition_month
                    {% for partition_field in additional_partition_by %}
                        , {{ partition_field }}
                    {% endfor %}
                    {% if not dim.name == "NAT" %},{{ dim.value_expr }} {% endif %}
                order by sum({{ ranking_metric }}) desc
            ) as {{ ranking_metric }}_ranked
        from {{ base_cte }}
        group by
            partition_month,
            updated_at,
            dimension_name,
            dimension_value
            {% for field in entity_fields %}
                , {{ field }}
            {% endfor %}
        qualify
            row_number() over (
                partition by
                    partition_month
                    {% for partition_field in additional_partition_by %}
                        , {{ partition_field }}
                    {% endfor %}
                    {% if not dim.name == "NAT" %},{{ dim.value_expr }} {% endif %}
                order by {{ ranking_metric }} desc
            )
            <= {{ top_n }}
    {% endfor %}
{% endmacro %}
