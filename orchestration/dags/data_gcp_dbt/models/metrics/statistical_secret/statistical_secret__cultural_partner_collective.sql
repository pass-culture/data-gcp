{{
    config(
        materialized="table",
        partition_by={
            "field": "partition_month",
            "data_type": "date",
            "granularity": "month",
        },
        cluster_by=["department_code", "epci_code", "municipality_code"],
    )
}}

with
    calendar_months as (
        select distinct
            date_trunc(partner_activation.partition_month, month) as partition_month
        from {{ ref("int_kpi__cultural_partner_activation") }} as partner_activation
        where partner_activation.partition_month >= '2022-01-01'
    ),

    base_venue_revenue as (
        select
            booking.venue_department_code as department_code,
            booking.venue_epci_code as epci_code,
            booking.venue_municipality_code as municipality_code,
            booking.venue_id,
            date_trunc(booking.collective_booking_used_date, month) as booking_month,
            sum(booking.booking_amount) as total_revenue_amount_venue
        from {{ ref("mrt_global__collective_booking") }} as booking
        where
            booking.is_used_collective_booking = true
            and booking.collective_booking_used_date >= '2021-07-01'
        group by
            date_trunc(booking.collective_booking_used_date, month),
            booking.venue_department_code,
            booking.venue_epci_code,
            booking.venue_municipality_code,
            booking.venue_id
    ),

    base_partner_activation as (
        select
            partner_activation.partner_department_code as department_code,
            partner_activation.partner_epci_code as epci_code,
            partner_activation.partner_municipality_code as municipality_code,
            date_trunc(partner_activation.partition_month, month) as partner_month,
            sum(
                partner_activation.total_active_partners_collective
            ) as total_active_partners_count
        from {{ ref("int_kpi__cultural_partner_activation") }} as partner_activation
        where partner_activation.partition_month >= '2021-07-01'
        group by
            date_trunc(partner_activation.partition_month, month),
            partner_activation.partner_department_code,
            partner_activation.partner_epci_code,
            partner_activation.partner_municipality_code
    ),

    dept_rolling_revenue as (
        select
            months.partition_month,
            revenue.department_code,
            sum(revenue.total_revenue_amount_venue) as total_revenue_amount,
            max(revenue.total_revenue_amount_venue) as max_revenue_amount
        from calendar_months as months
        inner join
            base_venue_revenue as revenue
            on months.partition_month >= revenue.booking_month
            and revenue.booking_month
            >= date_sub(months.partition_month, interval 5 month)
        where revenue.department_code is not null
        group by months.partition_month, revenue.department_code
    ),

    epci_rolling_revenue as (
        select
            months.partition_month,
            revenue.epci_code,
            sum(revenue.total_revenue_amount_venue) as total_revenue_amount,
            max(revenue.total_revenue_amount_venue) as max_revenue_amount
        from calendar_months as months
        inner join
            base_venue_revenue as revenue
            on months.partition_month >= revenue.booking_month
            and revenue.booking_month
            >= date_sub(months.partition_month, interval 5 month)
        where revenue.epci_code is not null
        group by months.partition_month, revenue.epci_code
    ),

    municipality_rolling_revenue as (
        select
            months.partition_month,
            revenue.municipality_code,
            sum(revenue.total_revenue_amount_venue) as total_revenue_amount,
            max(revenue.total_revenue_amount_venue) as max_revenue_amount
        from calendar_months as months
        inner join
            base_venue_revenue as revenue
            on months.partition_month >= revenue.booking_month
            and revenue.booking_month
            >= date_sub(months.partition_month, interval 5 month)
        where revenue.municipality_code is not null
        group by months.partition_month, revenue.municipality_code
    ),

    dept_rolling_partners as (
        select
            months.partition_month,
            partners.department_code,
            safe_divide(
                sum(partners.total_active_partners_count),
                count(distinct partners.partner_month)
            ) as avg_active_partners_count
        from calendar_months as months
        inner join
            base_partner_activation as partners
            on months.partition_month >= partners.partner_month
            and partners.partner_month
            >= date_sub(months.partition_month, interval 5 month)
        where partners.department_code is not null
        group by months.partition_month, partners.department_code
    ),

    epci_rolling_partners as (
        select
            months.partition_month,
            partners.epci_code,
            safe_divide(
                sum(partners.total_active_partners_count),
                count(distinct partners.partner_month)
            ) as avg_active_partners_count
        from calendar_months as months
        inner join
            base_partner_activation as partners
            on months.partition_month >= partners.partner_month
            and partners.partner_month
            >= date_sub(months.partition_month, interval 5 month)
        where partners.epci_code is not null
        group by months.partition_month, partners.epci_code
    ),

    municipality_rolling_partners as (
        select
            months.partition_month,
            partners.municipality_code,
            safe_divide(
                sum(partners.total_active_partners_count),
                count(distinct partners.partner_month)
            ) as avg_active_partners_count
        from calendar_months as months
        inner join
            base_partner_activation as partners
            on months.partition_month >= partners.partner_month
            and partners.partner_month
            >= date_sub(months.partition_month, interval 5 month)
        where partners.municipality_code is not null
        group by months.partition_month, partners.municipality_code
    ),

    geo_labels as (
        select
            geo.municipality_code,
            geo.epci_code,
            geo.department_code,
            max(geo.municipality_label) as municipality_label,
            max(geo.epci_label) as epci_label,
            max(geo.department_name) as department_label
        from {{ ref("int_seed__geo_iris") }} as geo
        where geo.municipality_code is not null
        group by geo.municipality_code, geo.epci_code, geo.department_code
    ),

    final_secret_evaluation as (
        select
            months.partition_month,
            geo.department_code,
            geo.department_label,
            geo.epci_code,
            geo.epci_label,
            geo.municipality_code,
            geo.municipality_label,

            coalesce(
                dept_partners.avg_active_partners_count <= 3, false
            ) as is_dept_secret_by_low_partners,
            coalesce(
                safe_divide(
                    dept_revenue.max_revenue_amount, dept_revenue.total_revenue_amount
                )
                > 0.85,
                false
            ) as is_dept_secret_by_high_concentration,

            coalesce(
                epci_partners.avg_active_partners_count <= 3, false
            ) as is_epci_secret_by_low_partners,
            coalesce(
                safe_divide(
                    epci_revenue.max_revenue_amount, epci_revenue.total_revenue_amount
                )
                > 0.85,
                false
            ) as is_epci_secret_by_high_concentration,

            coalesce(
                municipality_partners.avg_active_partners_count <= 3, false
            ) as is_municipality_secret_by_low_partners,
            coalesce(
                safe_divide(
                    municipality_revenue.max_revenue_amount,
                    municipality_revenue.total_revenue_amount
                )
                > 0.85,
                false
            ) as is_municipality_secret_by_high_concentration

        from calendar_months as months
        cross join geo_labels as geo

        left join
            dept_rolling_revenue as dept_revenue
            on months.partition_month = dept_revenue.partition_month
            and geo.department_code = dept_revenue.department_code
        left join
            epci_rolling_revenue as epci_revenue
            on months.partition_month = epci_revenue.partition_month
            and geo.epci_code = epci_revenue.epci_code
        left join
            municipality_rolling_revenue as municipality_revenue
            on months.partition_month = municipality_revenue.partition_month
            and geo.municipality_code = municipality_revenue.municipality_code

        left join
            dept_rolling_partners as dept_partners
            on months.partition_month = dept_partners.partition_month
            and geo.department_code = dept_partners.department_code
        left join
            epci_rolling_partners as epci_partners
            on months.partition_month = epci_partners.partition_month
            and geo.epci_code = epci_partners.epci_code
        left join
            municipality_rolling_partners as municipality_partners
            on months.partition_month = municipality_partners.partition_month
            and geo.municipality_code = municipality_partners.municipality_code
    )

select
    partition_month,
    department_code,
    department_label,
    epci_code,
    epci_label,
    municipality_code,
    municipality_label,

    (
        is_dept_secret_by_low_partners or is_dept_secret_by_high_concentration
    ) as is_department_secret,
    (
        is_epci_secret_by_low_partners or is_epci_secret_by_high_concentration
    ) as is_epci_secret,
    (
        is_municipality_secret_by_low_partners
        or is_municipality_secret_by_high_concentration
    ) as is_municipality_secret,

    case
        when is_dept_secret_by_low_partners and is_dept_secret_by_high_concentration
        then 'Les deux raisons (<= 3 partenaires ET > 85% CA)'
        when is_dept_secret_by_low_partners
        then 'Nombre de partenaires faible (<= 3)'
        when is_dept_secret_by_high_concentration
        then 'Concentration forte du CA (> 85%)'
        else 'Non soumis au secret'
    end as department_secret_reason,

    case
        when is_epci_secret_by_low_partners and is_epci_secret_by_high_concentration
        then 'Les deux raisons (<= 3 partenaires ET > 85% CA)'
        when is_epci_secret_by_low_partners
        then 'Nombre de partenaires faible (<= 3)'
        when is_epci_secret_by_high_concentration
        then 'Concentration forte du CA (> 85%)'
        else 'Non soumis au secret'
    end as epci_secret_reason,

    case
        when
            is_municipality_secret_by_low_partners
            and is_municipality_secret_by_high_concentration
        then 'Les deux raisons (<= 3 partenaires ET > 85% CA)'
        when is_municipality_secret_by_low_partners
        then 'Nombre de partenaires faible (<= 3)'
        when is_municipality_secret_by_high_concentration
        then 'Concentration forte du CA (> 85%)'
        else 'Non soumis au secret'
    end as municipality_secret_reason

from final_secret_evaluation
