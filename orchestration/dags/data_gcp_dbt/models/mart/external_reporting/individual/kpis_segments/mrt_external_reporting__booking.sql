{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_month", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = [
    {"name": "EPCI", "user_col": "user_epci", "venue_col": "venue_epci"},
    {"name": "COM", "user_col": "user_city", "venue_col": "venue_city"},
] %}

with
    base_booking_data as (
        select
            u.user_id,
            u.user_epci,
            u.user_city,
            b.venue_epci,
            b.venue_city,
            b.booking_id,
            b.booking_intermediary_amount,
            date_trunc(date(b.booking_used_date), month) as partition_month
        from {{ ref("mrt_global__booking") }} as b
        inner join
            {{ ref("mrt_global__user_beneficiary") }} as u on b.user_id = u.user_id
        where b.booking_status = 'REIMBURSED'
    ),

    free_bookable_offers as (
        select
            venue_city,
            venue_epci,
            date_trunc(date(offer_creation_date), month) as partition_month,
            count(distinct offer_id) as monthly_free_bookable_offers
        from {{ ref("mrt_global__offer") }}
        where last_stock_price = 0 and offer_is_bookable
        group by partition_month, venue_city, venue_epci
    ),

    cumul_free_bookable_offers as (
        select
            partition_month,
            venue_city,
            venue_epci,
            sum(monthly_free_bookable_offers) over (
                partition by venue_city, venue_epci
                order by partition_month
                rows unbounded preceding
            ) as cumul_total_free_offers
        from free_bookable_offers
    ),

    monthly_flows as (
        {% for dim in dimensions %}
            -- Vue du partenaire
            select
                partition_month,
                '{{ dim.name }}' as dimension_name,
                {{ dim.venue_col }} as dimension_value,
                'part_beneficiaires_dans_conso_partenaires_territoire' as kpi_base_name,
                sum(
                    case
                        when {{ dim.user_col }} = {{ dim.venue_col }}
                        then booking_intermediary_amount
                        else 0
                    end
                ) as monthly_num_amount,
                coalesce(sum(booking_intermediary_amount), 0) as monthly_den_amount,
                sum(
                    case when {{ dim.user_col }} = {{ dim.venue_col }} then 1 else 0 end
                ) as monthly_num_volume,
                count(booking_id) as monthly_den_volume
            from base_booking_data
            where {{ dim.venue_col }} is not null
            group by partition_month, dimension_name, dimension_value, kpi_base_name

            union all

            -- Vue du bénéficiaire
            select
                partition_month,
                '{{ dim.name }}' as dimension_name,
                {{ dim.user_col }} as dimension_value,
                'part_partenaires_dans_conso_beneficiaires_territoire' as kpi_base_name,
                sum(
                    case
                        when {{ dim.user_col }} = {{ dim.venue_col }}
                        then booking_intermediary_amount
                        else 0
                    end
                ) as monthly_num_amount,
                coalesce(sum(booking_intermediary_amount), 0) as monthly_den_amount,
                sum(
                    case when {{ dim.user_col }} = {{ dim.venue_col }} then 1 else 0 end
                ) as monthly_num_volume,
                count(booking_id) as monthly_den_volume
            from base_booking_data
            where {{ dim.user_col }} is not null
            group by partition_month, dimension_name, dimension_value, kpi_base_name

            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}
    ),

    cumulative_aggregation as (
        select
            partition_month,
            timestamp("{{ ts() }}") as updated_at,
            dimension_name,
            dimension_value,
            kpi_base_name,
            -- Cumul Montants
            sum(monthly_num_amount) over (
                partition by dimension_name, dimension_value, kpi_base_name
                order by partition_month
                rows unbounded preceding
            ) as cum_num_amount,
            sum(monthly_den_amount) over (
                partition by dimension_name, dimension_value, kpi_base_name
                order by partition_month
                rows unbounded preceding
            ) as cum_den_amount,
            -- Cumul Volumes
            sum(monthly_num_volume) over (
                partition by dimension_name, dimension_value, kpi_base_name
                order by partition_month
                rows unbounded preceding
            ) as cum_num_volume,
            sum(monthly_den_volume) over (
                partition by dimension_name, dimension_value, kpi_base_name
                order by partition_month
                rows unbounded preceding
            ) as cum_den_volume
        from monthly_flows
    ),

    final_output as (
        select
            partition_month,
            updated_at,
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
            updated_at,
            dimension_name,
            dimension_value,
            kpi_base_name || '_volume' as kpi_name,
            cum_num_volume as numerator,
            cum_den_volume as denominator,
            safe_divide(cum_num_volume, cum_den_volume) as kpi
        from cumulative_aggregation

        union all

        {% for dim in dimensions %}

            select
                partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.venue_col }} as dimension_value,
                'total_reservations_gratuites' as kpi_name,
                count(
                    case when booking_intermediary_amount = 0 then 1 end
                ) as numerator,
                cast(1 as numeric) as denominator,
                count(
                    case when booking_intermediary_amount = 0 then 1 end
                ) as kpi
            from base_booking_data
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name

            union all

            select
                partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.venue_col }} as dimension_value,
                'total_offres_gratuites_reservables' as kpi_name,
                cumul_total_free_offers as numerator,
                cast(1 as numeric) as denominator,
                cumul_total_free_offers as kpi
            from cumul_free_bookable_offers
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}

    )

select *
from final_output
{% if is_incremental() %}
    where
        partition_month
        = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
{% endif %}
