with artist_product_offers as (
select
    pal.artist_id,
    pal.artist_type,
    o.offer_product_id,
    o.offer_id,
    o.offer_category_id,
    o.offer_is_bookable,
    o.venue_id,
    o.partner_id,
    count(distinct case when not b.booking_is_cancelled then b.booking_id end) as total_bookings
from {{ ref("int_applicative__product_artist_link") }} as pal
left join {{ ref("int_global__offer") }} as o on pal.offer_product_id = o.offer_product_id
left join {{ ref("int_global__booking") }} as b on o.offer_id = b.offer_id
group by
    pal.artist_id,
    pal.artist_type,
    o.offer_product_id,
    o.offer_id,
    o.offer_is_bookable,
    o.offer_category_id,
    o.venue_id,
    o.partner_id
)

select
    a.artist_id,
    apo.artist_type,
    a.artist_name,
    a.artist_description,
    a.wikidata_image_file_url,
    a.wikidata_image_license,
    a.wikidata_image_license_url,
    a.wikidata_image_author,
    a.creation_date,
    a.modification_date,
    count(distinct apo.offer_product_id) as total_products,
    count(distinct apo.offer_id) as total_offers,
    count(distinct case when apo.offer_is_bookable then apo.offer_id end) as total_bookable_offers,
    count(distinct apo.offer_category_id) as total_offer_categories,
    count(distinct apo.venue_id) as total_venues,
    count(distinct apo.artist_type) as total_artist_types,
    sum(apo.total_bookings) as total_bookings
from {{ ref("int_applicative__artist") }} as a
left join artist_product_offers as apo on a.artist_id = apo.artist_id
group by
    a.artist_id,
    apo.artist_type,
    a.artist_name,
    a.artist_description,
    a.wikidata_image_file_url,
    a.wikidata_image_license,
    a.wikidata_image_license_url,
    a.wikidata_image_author,
    a.creation_date,
    a.modification_date
