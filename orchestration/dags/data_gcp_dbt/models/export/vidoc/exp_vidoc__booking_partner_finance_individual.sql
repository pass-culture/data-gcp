-- vidoc export of exp_base__booking_venue_individual: individual bookings and
-- amounts by venue territory, every published level (DEP, REG, GROUP, NAT),
-- under the statistical secret of businesses
select
    partition_month,
    territory_level,
    territory_code,
    territory_name,
    region_code,
    territory_group,
    is_statistic_secret,
    total_bookings,
    total_quantities,
    total_revenue_amount,
    total_reimbursed_amount,
    total_contribution_amount
from {{ ref("exp_base__booking_venue_individual") }}
