with
    artist_product_offers as (
        select
            pal.artist_id,
            pal.artist_type,
            o.offer_product_id,
            o.offer_id,
            o.offer_category_id,
            o.offer_is_bookable,
            o.venue_id,
            o.partner_id,
            count(
                distinct case when not b.booking_is_cancelled then b.booking_id end
            ) as total_bookings
        from {{ ref("int_applicative__product_artist_link") }} as pal
        left join
            {{ ref("int_global__offer") }} as o
            on pal.offer_product_id = o.offer_product_id
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
    ),

    artist_consultations as (
        select
            artist_id,
            origin,
            count(distinct unique_session_id) as total_consultations,
            count(distinct user_id) as total_consulted_users
        from {{ ref("int_firebase__native_event") }}
        where event_name = "ConsultArtist" and event_date >= date("2025-01-01")
        group by artist_id, origin
    )

select
    a.artist_id,
    a.artist_name,
    a.artist_description,
    a.wikidata_image_file_url,
    a.wikidata_image_license,
    a.wikidata_image_license_url,
    a.wikidata_image_author,
    a.creation_date as artist_creation_date,
    a.modification_date as artist_modification_date,
    coalesce(sum(ac.total_consultations),0) as artist_total_consultations,
    coalesce(sum(case when ac.origin = "search" then ac.total_consultations end),0) as artist_total_consultations_from_search,
    coalesce(sum(case when ac.origin = "offer" then ac.total_consultations end),0) as artist_total_consultations_from_offer,
    coalesce(sum(case when ac.origin = "venue" then ac.total_consultations end),0) as artist_total_consultations_from_venue,
    coalesce(sum(case when ac.origin = "searchAutoComplete" then ac.total_consultations end),0) as artist_total_consultations_from_search_auto_complete,
    coalesce(sum(ac.total_consulted_users),0) as artist_total_consulted_users,
    count(distinct apo.offer_product_id) as artist_total_products,
    count(distinct apo.offer_id) as artist_total_offers,
    count(distinct case when apo.offer_is_bookable then apo.offer_id end) as artist_total_bookable_offers,
    count(distinct apo.offer_category_id) as artist_total_offer_categories,
    count(distinct apo.venue_id) as artist_total_venues,
    count(distinct apo.artist_type) as artist_total_artist_types,
    coalesce(sum(apo.total_bookings), 0) as artist_total_bookings
from {{ ref("int_applicative__artist") }} as a
left join artist_product_offers as apo on a.artist_id = apo.artist_id
left join artist_consultations as ac on a.artist_id = ac.artist_id
group by
    a.artist_id,
    a.artist_name,
    a.artist_description,
    a.wikidata_image_file_url,
    a.wikidata_image_license,
    a.wikidata_image_license_url,
    a.wikidata_image_author,
    a.creation_date,
    a.modification_date
