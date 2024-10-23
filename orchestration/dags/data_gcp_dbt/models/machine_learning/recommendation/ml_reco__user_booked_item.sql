select distinct b.user_id as user_id, o.item_id as item_id
from {{ ref("int_applicative__booking") }} b
-- TODO add join in int_applicative__booking
join {{ ref("int_applicative__stock") }} s on s.stock_id = b.stock_id
join {{ ref("int_applicative__offer_item_id") }} o on o.offer_id = s.offer_id
join {{ ref("user_beneficiary") }} eud on eud.user_id = b.user_id
where booking_is_cancelled = false
