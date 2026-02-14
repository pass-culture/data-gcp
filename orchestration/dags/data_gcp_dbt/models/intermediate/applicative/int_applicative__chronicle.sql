-- WARNING: One chronicle can be linked to multiple products.
-- This join generates duplicate chronicle rows (one row per chronicle-product pair).
-- Use COUNT(DISTINCT chronicle_id) in downstream models to count unique chronicles.
select
    c.chronicle_id,

    c.user_id,
    c.chronicle_is_active,
    c.chronicle_content,
    c.chronicle_club_type,
    pc.product_id,
    date(c.chronicle_created_at) as chronicle_creation_date
from {{ ref("raw_applicative__chronicle") }} as c
left join
    {{ ref("raw_applicative__product_chronicle") }} as pc
    on c.chronicle_id = pc.chronicle_id
