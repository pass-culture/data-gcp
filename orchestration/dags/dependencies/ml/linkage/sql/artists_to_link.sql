with
    authors_table as (
        select
            author as artist_name,
            offer_category_id,
            is_synchronised,
            count(distinct(offer_id)) as offer_number,
            count(distinct(item_id)) as item_number,
            sum(ifnull(total_individual_bookings, 0)) as total_booking_count,
            'author' as artist_type
        from `{{ bigquery_analytics_dataset }}`.global_offer
        where
            offer_category_id
            in ("CINEMA", "MUSIQUE_LIVE", "SPECTACLE", "MUSIQUE_ENREGISTREE", "LIVRE")
            and author is not null
            and author != ""
        group by author, offer_category_id, is_synchronised
    ),
    performers_table as (
        select
            performer as artist_name,
            offer_category_id,
            is_synchronised,
            count(distinct(offer_id)) as offer_number,
            count(distinct(item_id)) as item_number,
            sum(ifnull(total_individual_bookings, 0)) as total_booking_count,
            'performer' as artist_type
        from `{{ bigquery_analytics_dataset }}`.global_offer
        where
            offer_category_id in ("MUSIQUE_LIVE", "SPECTACLE", "MUSIQUE_ENREGISTREE")
            and performer is not null
            and performer != ""
        group by performer, offer_category_id, is_synchronised
    )
select *
from authors_table
union all
select *
from performers_table
