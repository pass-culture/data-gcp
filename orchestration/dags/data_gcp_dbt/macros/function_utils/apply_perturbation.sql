{% macro apply_perturbation(count_col, output_alias, join_alias) %}
    greatest({{ count_col }} + {{ join_alias }}.perturbation, 0) as {{ output_alias }}
{% endmacro %}

{% macro perturbation_join(join_alias, count_col, cell_key_col) %}
    inner join
        {{ ref("perturbation_table__individual") }} as {{ join_alias }}
        on {{ count_col }}
        between {{ join_alias }}.count_min and {{ join_alias }}.count_max
        and {{ cell_key_col }}
        between {{ join_alias }}.cell_key_min and {{ join_alias }}.cell_key_max
{% endmacro %}
