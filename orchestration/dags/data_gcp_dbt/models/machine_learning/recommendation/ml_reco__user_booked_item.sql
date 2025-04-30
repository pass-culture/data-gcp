select distinct b.user_id, o.item_id
from {{ ref("int_applicative__booking") }} as b
-- TODO add join in int_applicative__booking
inner join {{ ref("int_applicative__stock") }} as s on b.stock_id = s.stock_id
inner join {{ ref("int_applicative__offer_item_id") }} as o on s.offer_id = o.offer_id
inner join {{ ref("int_applicative__user") }} as eud on b.user_id = eud.user_id
where booking_is_cancelled = false
