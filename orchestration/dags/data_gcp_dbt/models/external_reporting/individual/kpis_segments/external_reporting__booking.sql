-- noqa: disable=all
{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{#
    Roll-up of the individual booking and free offer metrics to the reporting
    dimensions:
    - shares of the bookings that stay within the territory, seen from the
      cultural partners (venue geography) and from the beneficiaries (user
      geography), cumulative since the first month of use; no national value;
      bookings of users missing from mrt_global__user_beneficiary (deleted,
      suspended, GRANT_FREE) are left out;
    - free bookings and all bookings by creation month, cancelled ones
      included (free offers are often cancelled by default as they need no
      check), bookings of every user, and the cumulative stock of free offers,
      venue geography.
#}
{% set venue_dims = get_dimensions("venue", "geo_full") %}
{% set user_cols = {} %}
{% for dim in get_dimensions("beneficiary", "geo_full") %}
    {% do user_cols.update({dim.name: dim.value_expr}) %}
{% endfor %}
{% set last_month = (
    "date_trunc(date_sub(date('" ~ ds() ~ "'), interval 1 month), month)"
) %}

with
    monthly_flows as (
        {% for dim in venue_dims if dim.name != "NAT" %}
            {% set user_col = user_cols[dim.name] %}
            {% set first_dim = loop.first %}
            {% for side in [
                {
                    "kpi": "part_beneficiaires_dans_conso_partenaires_territoire",
                    "col": dim.value_expr,
                },
                {
                    "kpi": "part_partenaires_dans_conso_beneficiaires_territoire",
                    "col": user_col,
                },
            ] %}
                {% if not (first_dim and loop.first) %}
                    union all
                {% endif %}
                select
                    partition_month,
                    '{{ dim.name }}' as dimension_name,
                    {{ side.col }} as dimension_value,
                    '{{ side.kpi }}' as kpi_base_name,
                    sum(
                        if({{ user_col }} = {{ dim.value_expr }}, total_used_amount, 0)
                    ) as monthly_num_amount,
                    coalesce(sum(total_used_amount), 0) as monthly_den_amount,
                    sum(
                        if(
                            {{ user_col }} = {{ dim.value_expr }},
                            total_used_bookings,
                            0
                        )
                    ) as monthly_num_volume,
                    sum(total_used_bookings) as monthly_den_volume
                from {{ ref("metrics_booking__individual") }}
                where
                    user_is_beneficiary
                    and total_used_bookings > 0
                    and {{ side.col }} is not null
                group by partition_month, dimension_name, dimension_value, kpi_base_name
            {% endfor %}
        {% endfor %}
    ),

    cumulative_aggregation as (
        select
            partition_month,
            dimension_name,
            dimension_value,
            kpi_base_name,
            sum(monthly_num_amount) over w as cum_num_amount,
            sum(monthly_den_amount) over w as cum_den_amount,
            sum(monthly_num_volume) over w as cum_num_volume,
            sum(monthly_den_volume) over w as cum_den_volume
        from monthly_flows
        window
            w as (
                partition by dimension_name, dimension_value, kpi_base_name
                order by partition_month
                rows unbounded preceding
            )
    ),

    free_kpis as (
        {% for dim in venue_dims %}
            {% if not loop.first %}
                union all
            {% endif %}
            select
                partition_month,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                sum(total_created_free_bookings) as total_reservations_gratuites,
                sum(total_created_bookings) as total_reservations_toutes,
                null as total_offres_gratuites_reservables,
                null as total_offres_gratuites
            from {{ ref("metrics_booking__individual") }}
            where total_created_bookings > 0
            group by partition_month, dimension_name, dimension_value

            union all

            -- the stock of free offers is published up to the last complete month
            select
                partition_month,
                '{{ dim.name }}' as dimension_name,
                {{ dim.value_expr }} as dimension_value,
                null as total_reservations_gratuites,
                null as total_reservations_toutes,
                sum(
                    cumulative_total_created_free_bookable_offers
                ) as total_offres_gratuites_reservables,
                sum(cumulative_total_created_free_offers) as total_offres_gratuites
            from {{ ref("metrics_offer__individual_free") }}
            where partition_month <= {{ last_month }}
            group by partition_month, dimension_name, dimension_value
        {% endfor %}
    ),

    final_output as (
        select
            partition_month,
            dimension_name,
            dimension_value,
            kpi_base_name || '_montant' as kpi_name,
            cum_num_amount as numerator,
            cum_den_amount as denominator,
            safe_divide(cum_num_amount, cum_den_amount) as kpi
        from cumulative_aggregation

        union all

        select
            partition_month,
            dimension_name,
            dimension_value,
            kpi_base_name || '_volume' as kpi_name,
            cum_num_volume as numerator,
            cum_den_volume as denominator,
            safe_divide(cum_num_volume, cum_den_volume) as kpi
        from cumulative_aggregation

        union all

        select
            partition_month,
            dimension_name,
            dimension_value,
            kpi_name,
            numerator,
            cast(1 as numeric) as denominator,
            numerator as kpi
        from
            free_kpis unpivot (
                numerator for kpi_name in (
                    total_reservations_gratuites,
                    total_reservations_toutes,
                    total_offres_gratuites_reservables,
                    total_offres_gratuites
                )
            )
    )

select
    partition_month,
    timestamp('{{ ts() }}') as updated_at,
    dimension_name,
    dimension_value,
    kpi_name,
    numerator,
    denominator,
    kpi
from final_output
where
    dimension_value is not null
    {% if is_incremental() %} and partition_month = {{ last_month }} {% endif %}
