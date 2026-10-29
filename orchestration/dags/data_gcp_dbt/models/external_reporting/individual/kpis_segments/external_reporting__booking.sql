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

{% set dimensions = [
    {
        "name": "REG",
        "user_col": "user_region_name",
        "venue_col": "venue_region_name",
    },
    {
        "name": "DEP",
        "user_col": "user_department_name",
        "venue_col": "venue_department_name",
    },
    {
        "name": "EPCI",
        "user_col": "user_epci_code",
        "venue_col": "venue_epci_code",
    },
    {
        "name": "COM",
        "user_col": "user_municipality_code",
        "venue_col": "venue_municipality_code",
    },
] %}

with
    base_booking_data as (
        select
            u.user_id,
            u.user_region_name,
            u.user_department_name,
            u.user_epci,
            u.user_epci_code,
            u.user_city,
            u.user_municipality_code,
            b.venue_region_name,
            b.venue_department_name,
            b.venue_epci,
            b.venue_epci_code,
            b.venue_city,
            b.venue_municipality_code,
            b.booking_id,
            b.booking_intermediary_amount,
            date_trunc(date(b.booking_used_date), month) as partition_month
        from {{ ref("mrt_global__booking") }} as b
        inner join
            {{ ref("mrt_global__user_beneficiary") }} as u on b.user_id = u.user_id
        where b.booking_is_used
    ),

    -- free offers (last stock price 0) created by month, in any state and
    -- currently bookable
    free_offers as (
        select
            venue_municipality_code,
            venue_epci_code,
            venue_region_name,
            venue_department_name,
            date_trunc(date(offer_creation_date), month) as partition_month,
            count(distinct offer_id) as monthly_free_offers,
            count(
                distinct if(offer_is_bookable, offer_id, null)
            ) as monthly_free_bookable_offers
        from {{ ref("mrt_global__offer") }}
        where last_stock_price = 0
        group by
            partition_month,
            venue_municipality_code,
            venue_epci_code,
            venue_region_name,
            venue_department_name
    ),

    -- every month from the first free offer of the territory, so that the
    -- cumulative stock exists every month and sums correctly across territories
    free_offers_grid as (
        select
            territories.venue_municipality_code,
            territories.venue_epci_code,
            territories.venue_region_name,
            territories.venue_department_name,
            partition_month
        from
            (
                select
                    venue_municipality_code,
                    venue_epci_code,
                    venue_region_name,
                    venue_department_name,
                    min(partition_month) as first_month
                from free_offers
                group by
                    venue_municipality_code,
                    venue_epci_code,
                    venue_region_name,
                    venue_department_name
            ) as territories
        cross join
            unnest(
                generate_date_array(
                    territories.first_month,
                    date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month),
                    interval 1 month
                )
            ) as partition_month
    ),

    cumul_free_bookable_offers as (
        select
            grid.partition_month,
            grid.venue_municipality_code,
            grid.venue_epci_code,
            grid.venue_region_name,
            grid.venue_department_name,
            sum(coalesce(free_offers.monthly_free_offers, 0)) over w
            as cumul_total_free_offers,
            sum(coalesce(free_offers.monthly_free_bookable_offers, 0)) over w
            as cumul_total_free_bookable_offers
        from free_offers_grid as grid
        left join
            free_offers
            on grid.partition_month = free_offers.partition_month
            and grid.venue_municipality_code
            is not distinct from free_offers.venue_municipality_code
            and grid.venue_epci_code is not distinct from free_offers.venue_epci_code
            and grid.venue_region_name
            is not distinct from free_offers.venue_region_name
            and grid.venue_department_name
            is not distinct from free_offers.venue_department_name
        window
            w as (
                partition by
                    grid.venue_municipality_code,
                    grid.venue_epci_code,
                    grid.venue_region_name,
                    grid.venue_department_name
                order by grid.partition_month
                rows unbounded preceding
            )
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

        -- free offers and bookings are also published at NAT (the shares above
        -- compare two territories and have no national value)
        {% for dim in [{"name": "NAT", "venue_col": "'NAT'"}] + dimensions %}

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
                count(case when booking_intermediary_amount = 0 then 1 end) as kpi
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
                sum(cumul_total_free_bookable_offers) as numerator,
                cast(1 as numeric) as denominator,
                sum(cumul_total_free_bookable_offers) as kpi
            from cumul_free_bookable_offers
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name

            union all

            select
                partition_month,
                timestamp("{{ ts() }}") as updated_at,
                '{{ dim.name }}' as dimension_name,
                {{ dim.venue_col }} as dimension_value,
                'total_offres_gratuites' as kpi_name,
                sum(cumul_total_free_offers) as numerator,
                cast(1 as numeric) as denominator,
                sum(cumul_total_free_offers) as kpi
            from cumul_free_bookable_offers
            group by
                partition_month, updated_at, dimension_name, dimension_value, kpi_name
            {% if not loop.last %}
                union all
            {% endif %}
        {% endfor %}

    )

select *
from final_output
where
    dimension_value is not null
    {% if is_incremental() %}
        and partition_month
        = date_trunc(date_sub(date("{{ ds() }}"), interval 1 month), month)
    {% endif %}
