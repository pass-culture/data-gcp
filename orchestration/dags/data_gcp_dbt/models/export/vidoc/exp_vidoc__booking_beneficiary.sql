-- vidoc export of exp_base__booking_beneficiary: used individual bookings and
-- amounts by territory of residence of the beneficiaries, all categories
-- ('ALL') and by category, every published level (DEP, REG, GROUP, NAT), low
-- count rule applied in the base
select
    partition_month,
    territory_level,
    territory_code,
    territory_name,
    region_code,
    territory_group,
    offer_category_id,
    is_low_count,
    total_used_bookings,
    total_used_amount
from {{ ref("exp_base__booking_beneficiary") }}
