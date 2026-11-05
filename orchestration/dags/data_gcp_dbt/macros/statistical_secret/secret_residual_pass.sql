{#
    Territory group of a department code: METRO (metropolitan France, Corsica
    included), DROM (overseas departments and regions) or COM (overseas
    collectivities).
#}
{% macro territory_group(department_code) %}
    case
        when {{ department_code }} in ('971', '972', '973', '974', '976')
        then 'DROM'
        when length({{ department_code }}) = 3
        then 'COM'
        else 'METRO'
    end
{% endmacro %}

{#
    True when a set of partners with these totals passes the primary secret
    rules (at least min_partners_threshold partners, no partner above
    max_concentration_threshold of the revenue). NULL when the revenue is 0.
#}
{% macro passes_primary_secret(
    partners,
    revenue,
    max_venue_revenue,
    min_partners_threshold,
    max_concentration_threshold
) %}
    (
        {{ partners }} >= {{ min_partners_threshold }}
        and safe_divide({{ max_venue_revenue }}, {{ revenue }})
        <= {{ max_concentration_threshold }}
    )
{% endmacro %}

{#
    Secondary secret of the departments within a parent total (region or
    territory group). The masked departments of a parent can be recovered by
    subtracting the visible ones from the published parent total: their
    combined revenue must pass the primary rules. When it does not, the active
    visible departments of the parent are added to the masked set from the
    smallest revenue up, until it passes. When it never passes, all of them are
    masked (the parent total is then secret, see is_region_secret).

    input_cte columns: partition_month, department_code, <parent_column>,
    partners, revenue, max_venue_revenue and the boolean <masked_column>.
    Returns the CTEs, the last one named <name> with the input columns and the
    boolean <output_column>. No trailing comma.
#}
{% macro secret_residual_pass(
    name,
    input_cte,
    parent_column,
    masked_column,
    output_column,
    min_partners_threshold,
    max_concentration_threshold
) %}
    {{ name }}_masked as (
        select
            partition_month,
            {{ parent_column }},
            sum(partners) as masked_partners,
            sum(revenue) as masked_revenue,
            max(max_venue_revenue) as masked_max_venue_revenue
        from {{ input_cte }}
        where {{ masked_column }}
        group by partition_month, {{ parent_column }}
    ),

    {{ name }}_candidates as (
        select
            partition_month,
            department_code,
            {{ parent_column }},
            row_number() over (
                partition by partition_month, {{ parent_column }}
                order by revenue asc, department_code asc
            ) as candidate_rank,
            sum(partners) over cumulative as cumulative_partners,
            sum(revenue) over cumulative as cumulative_revenue,
            max(max_venue_revenue) over cumulative as cumulative_max_venue_revenue
        from {{ input_cte }}
        where not {{ masked_column }} and revenue > 0
        window
            cumulative as (
                partition by partition_month, {{ parent_column }}
                order by revenue asc, department_code asc
                rows between unbounded preceding and current row
            )
    ),

    {{ name }}_cut as (
        select
            masked.partition_month,
            masked.{{ parent_column }},
            coalesce(
                {{
                    passes_primary_secret(
                        "masked.masked_partners",
                        "masked.masked_revenue",
                        "masked.masked_max_venue_revenue",
                        min_partners_threshold,
                        max_concentration_threshold,
                    )
                }},
                false
            ) as is_residual_passing,
            min(
                if(
                    coalesce(
                        {{
                            passes_primary_secret(
                                "masked.masked_partners + candidates.cumulative_partners",
                                "masked.masked_revenue + candidates.cumulative_revenue",
                                "greatest(masked.masked_max_venue_revenue, candidates.cumulative_max_venue_revenue)",
                                min_partners_threshold,
                                max_concentration_threshold,
                            )
                        }},
                        false
                    ),
                    candidates.candidate_rank,
                    null
                )
            ) as cut_rank
        from {{ name }}_masked as masked
        left join
            {{ name }}_candidates as candidates
            on masked.partition_month = candidates.partition_month
            and masked.{{ parent_column }} = candidates.{{ parent_column }}
        group by
            masked.partition_month,
            masked.{{ parent_column }},
            masked.masked_partners,
            masked.masked_revenue,
            masked.masked_max_venue_revenue
    ),

    {{ name }} as (
        select
            input.*,
            input.{{ masked_column }}
            or coalesce(
                not cut.is_residual_passing
                and candidates.candidate_rank <= coalesce(cut.cut_rank, 2147483647),
                false
            ) as {{ output_column }}
        from {{ input_cte }} as input
        left join
            {{ name }}_candidates as candidates
            on input.partition_month = candidates.partition_month
            and input.department_code = candidates.department_code
        left join
            {{ name }}_cut as cut
            on input.partition_month = cut.partition_month
            and input.{{ parent_column }} = cut.{{ parent_column }}
    )
{% endmacro %}
