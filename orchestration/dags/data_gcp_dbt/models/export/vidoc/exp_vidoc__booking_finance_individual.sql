select
    partition_month,
    venue_department_code,
    venue_department_name,
    venue_region_name,
    venue_epci_code,
    venue_city_code,
    offerer_is_epn,
    offer_category_id,
    is_statistic_secret,
    case when not is_statistic_secret then total_bookings end as total_bookings,
    case when not is_statistic_secret then total_quantities end as total_quantities,
    case
        when not is_statistic_secret then total_revenue_amount
    end as total_revenue_amount,
    case
        when not is_statistic_secret then total_reimbursed_amount
    end as total_reimbursed_amount,
    case
        when not is_statistic_secret then total_contribution_amount
    end as total_contribution_amount
from {{ ref("metrics_booking__finance_individual") }}
