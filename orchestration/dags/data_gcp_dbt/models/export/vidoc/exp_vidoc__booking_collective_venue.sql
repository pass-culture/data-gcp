-- vidoc export of exp_base__booking_venue_collective: collective bookings and
-- amounts by venue territory, every published level (DEP, REG, GROUP, NAT),
-- under the statistical secret of businesses
select
    partition_month,
    scholar_year,
    territory_level,
    territory_code,
    territory_name,
    region_code,
    territory_group,
    is_statistic_secret,
    total_collective_bookings,
    total_collective_amount_spent
from {{ ref("exp_base__booking_venue_collective") }}
