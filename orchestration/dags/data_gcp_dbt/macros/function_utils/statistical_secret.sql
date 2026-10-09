-- Primary statistical secret rule for business data (INSEE): a cell is secret
-- when it has fewer than 3 contributors or when a single contributor accounts
-- for 85% or more of the cell value.
{% macro is_statistical_secret(n_contributors, top_contributor_share) %}
    (
        {{ n_contributors }} < {{ var("statistical_secret_min_contributors") }}
        or coalesce(
            {{ top_contributor_share }}
            >= {{ var("statistical_secret_max_dominance_share") }},
            false
        )
    )
{% endmacro %}

-- Secondary suppression: for each published parent cell, find how many
-- children (ranked by ascending revenue) must be hidden so that the residual
-- "parent minus published children" passes the primary rule.
-- `contributions` must expose: partition_month, booking_type, parent_code,
-- candidate_rank (0 = already uncovered, 1..n = published children by ascending
-- revenue), offerer_id, revenue_amount.
-- Returns hidden_rank: children with candidate_rank <= hidden_rank are hidden.
-- An empty residual needs no hiding; when no prefix up to max_candidates is
-- enough, every child is hidden.
{% macro secondary_suppression_rank(contributions, max_candidates=5) %}
    select
        partition_month,
        booking_type,
        parent_code,
        case
            when min(hidden_rank) > 0
            then 0
            else
                coalesce(
                    min(if(not is_secret, hidden_rank, null)),
                    {{ var("statistical_secret_hide_all_rank") }}
                )
        end as hidden_rank
    from
        (
            select
                partition_month,
                booking_type,
                parent_code,
                hidden_rank,
                {{
                    is_statistical_secret(
                        "count(*)",
                        "safe_divide(max(offerer_revenue_amount), sum(offerer_revenue_amount))",
                    )
                }}
                as is_secret
            from
                (
                    select
                        contribution.partition_month,
                        contribution.booking_type,
                        contribution.parent_code,
                        hidden_rank,
                        contribution.offerer_id,
                        sum(contribution.revenue_amount) as offerer_revenue_amount
                    from {{ contributions }} as contribution
                    cross join
                        unnest(generate_array(0, {{ max_candidates }})) as hidden_rank
                    where contribution.candidate_rank <= hidden_rank
                    group by all
                )
            group by all
        )
    group by partition_month, booking_type, parent_code
{% endmacro %}
