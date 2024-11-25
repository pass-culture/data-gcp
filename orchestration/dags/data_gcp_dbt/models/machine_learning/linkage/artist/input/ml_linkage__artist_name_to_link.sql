with
    authors_table as (
        select
            author as artist_name,
            offer_category_id,
            is_synchronised as offer_is_synchronised,
            count(distinct offer_id) as total_offer_count,
            count(distinct item_id) as total_item_count,
            sum(ifnull(total_individual_bookings, 0)) as total_booking_count,
            'author' as artist_type
        from {{ ref("mrt_global__offer") }}
        where
            offer_category_id
            in ("CINEMA", "MUSIQUE_LIVE", "SPECTACLE", "MUSIQUE_ENREGISTREE", "LIVRE")
            and author is not null
            and author != ""
        group by author, offer_category_id, offer_is_synchronised
    ),
    performers_table as (
        select
            performer as artist_name,
            offer_category_id,
            is_synchronised as offer_is_synchronised,
            count(distinct offer_id) as total_offer_count,
            count(distinct item_id) as total_item_count,
            sum(ifnull(total_individual_bookings, 0)) as total_booking_count,
            'performer' as artist_type
        from {{ ref("mrt_global__offer") }}
        where
            offer_category_id in ("MUSIQUE_LIVE", "SPECTACLE", "MUSIQUE_ENREGISTREE")
            and performer is not null
            and performer != ""
        group by performer, offer_category_id, offer_is_synchronised
    )
select
    artist_name,
    offer_category_id,
    offer_is_synchronised,
    total_offer_count,
    total_item_count,
    total_booking_count,
    artist_type
from authors_table
union all
select
    artist_name,
    offer_category_id,
    offer_is_synchronised,
    total_offer_count,
    total_item_count,
    total_booking_count,
    artist_type
from performers_table
