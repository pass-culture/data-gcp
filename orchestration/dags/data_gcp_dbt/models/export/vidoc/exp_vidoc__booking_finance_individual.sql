with
    -- 1. Agrégation des métriques financières au grain Départemental
    aggregated_department_finance as (
        select
            partition_month,
            venue_department_code,
            venue_department_name,
            venue_region_name,
            sum(total_bookings) as total_bookings,
            sum(total_quantities) as total_quantities,
            sum(total_revenue_amount) as total_revenue_amount,
            sum(total_reimbursed_amount) as total_reimbursed_amount,
            sum(total_contribution_amount) as total_contribution_amount
        from {{ ref("metrics_booking__finance_individual") }}
        group by
            partition_month,
            venue_department_code,
            venue_department_name,
            venue_region_name
    ),

    -- 2. Isolation de la grille de secret unique au grain Département pour la fenêtre voulue
    department_secret_grid as (
        select distinct
            partition_month,
            department_code,
            is_department_secret,
            department_secret_reason_code,
            department_secret_reason_label
        from {{ ref("statistical_secret__cultural_partner_individual") }}
        where rolling_window_months_count = 6
    )

select
    facts.partition_month,
    facts.venue_department_code,
    facts.venue_department_name,
    facts.venue_region_name,

    -- Masquage conditionnel des métriques en cas de secret
    coalesce(sec.is_department_secret, true) as is_statistic_secret,
    sec.department_secret_reason_code,
    sec.department_secret_reason_label,

    case
        when sec.is_department_secret is not true then facts.total_bookings
    end as total_bookings,

    case
        when sec.is_department_secret is not true then facts.total_quantities
    end as total_quantities,

    case
        when sec.is_department_secret is not true then facts.total_revenue_amount
    end as total_revenue_amount,

    case
        when sec.is_department_secret is not true then facts.total_reimbursed_amount
    end as total_reimbursed_amount,

    case
        when sec.is_department_secret is not true then facts.total_contribution_amount
    end as total_contribution_amount

from aggregated_department_finance as facts

left join department_secret_grid as sec
    on sec.partition_month = facts.partition_month
   and sec.department_code = facts.venue_department_code