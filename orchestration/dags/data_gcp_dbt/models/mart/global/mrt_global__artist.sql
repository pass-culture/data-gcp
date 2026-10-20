with
    -- Categories handled for artist categorization. Extend list to add more.
    supported_artist_categorization_search_group_names as (
        select search_group_name
        from unnest(["LIVRES", "CINEMA", "MUSIQUE"]) as search_group_name
    ),

    artist_product_offers as (
        select
            pal.artist_id,
            pal.artist_type,
            o.offer_product_id,
            o.offer_id,
            o.offer_category_id,
            o.search_group_name,
            o.offer_is_bookable,
            o.venue_id,
            o.partner_id,
            count(
                distinct case when not b.booking_is_cancelled then b.booking_id end
            ) as total_bookings,
            -- Rolling 30-day booking count, used for "trending" ranking scores.
            count(
                distinct case
                    when
                        not b.booking_is_cancelled
                        and b.booking_creation_date
                        >= date_sub(current_date(), interval 30 day)
                    then b.booking_id
                end
            ) as total_bookings_last_30d
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
            o.search_group_name,
            o.venue_id,
            o.partner_id
    ),

    -- Offer count per artist/search_group_name (all offers, not just bookable ones)
    -- used to determine main/associated categories.
    artist_offer_counts_per_search_group_name as (
        select
            apo.artist_id,
            apo.search_group_name,
            count(distinct apo.offer_id) as total_offers,
            count(
                distinct case when apo.offer_is_bookable then apo.offer_id end
            ) as total_bookable_offers
        from artist_product_offers as apo
        inner join
            supported_artist_categorization_search_group_names as hsg
            on apo.search_group_name = hsg.search_group_name
        group by apo.artist_id, apo.search_group_name
    ),

    artist_search_group_names_ranked_by_offer_count as (
        select
            artist_id,
            search_group_name,
            total_offers,
            total_bookable_offers,
            row_number() over (
                partition by artist_id order by total_offers desc, search_group_name asc
            ) as category_rank
        from artist_offer_counts_per_search_group_name
    ),

    artist_main_search_group_name_and_bookable_flag as (
        select
            artist_id,
            search_group_name as main_search_group_name,
            total_bookable_offers > 0 as main_search_group_name_has_bookable_offer
        from artist_search_group_names_ranked_by_offer_count
        where category_rank = 1
    ),

    artist_associated_search_group_names_array as (
        select
            artist_id,
            array_agg(
                search_group_name order by category_rank
            ) as associated_search_group_names
        from artist_search_group_names_ranked_by_offer_count
        where category_rank > 1
        group by artist_id
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
    ),

    -- Rolling 30-day offer-page consultations (ConsultOffer), per artist/category.
    artist_offer_consultation_counts_last_30d_per_search_group_name as (
        select
            apo.artist_id,
            apo.search_group_name,
            count(distinct e.unique_session_id) as total_consultations_last_30d
        from artist_product_offers as apo
        inner join
            {{ ref("int_firebase__native_consultation") }} as e
            on apo.offer_id = e.offer_id
        where e.consultation_date >= date_sub(current_date(), interval 30 day)
        group by apo.artist_id, apo.search_group_name
    ),

    artist_booking_counts_last_30d_per_search_group_name as (
        select
            artist_id,
            search_group_name,
            sum(total_bookings_last_30d) as total_bookings_last_30d
        from artist_product_offers
        group by artist_id, search_group_name
    )

select
    a.artist_id,
    a.artist_name,
    a.normalized_artist_name,
    a.artist_description,
    a.artist_biography,
    a.artist_mediation_uuid,
    a.artist_computed_image,
    a.wikidata_id,
    a.wikipedia_url,
    a.wikidata_image_file_url,
    a.wikidata_image_license,
    a.wikidata_image_license_url,
    a.wikidata_image_author,
    a.artist_app_search_score,
    a.artist_pro_search_score,
    a.creation_date as artist_creation_date,
    a.modification_date as artist_modification_date,
    amc.main_search_group_name,
    amc.main_search_group_name_has_bookable_offer,
    any_value(aac.associated_search_group_names) as associated_search_group_names,
    coalesce(sum(ac.total_consultations), 0) as artist_total_consultations,
    coalesce(
        sum(case when ac.origin = "search" then ac.total_consultations end), 0
    ) as artist_total_consultations_from_search,
    coalesce(
        sum(case when ac.origin = "offer" then ac.total_consultations end), 0
    ) as artist_total_consultations_from_offer,
    coalesce(
        sum(case when ac.origin = "venue" then ac.total_consultations end), 0
    ) as artist_total_consultations_from_venue,
    coalesce(
        sum(case when ac.origin = "searchAutoComplete" then ac.total_consultations end),
        0
    ) as artist_total_consultations_from_search_auto_complete,
    coalesce(sum(ac.total_consulted_users), 0) as artist_total_consulted_users,
    count(distinct apo.offer_product_id) as artist_total_products,
    count(distinct apo.offer_id) as artist_total_offers,
    count(
        distinct case when apo.offer_is_bookable then apo.offer_id end
    ) as artist_total_bookable_offers,
    count(distinct apo.offer_category_id) as artist_total_offer_categories,
    count(distinct apo.venue_id) as artist_total_venues,
    count(distinct apo.artist_type) as artist_total_artist_types,
    coalesce(sum(apo.total_bookings), 0) as artist_total_bookings,
    -- Bookings/consultations over the last 30 days on the main category, feeding
    -- the "trending" score in ml_metadata__artist_score.
    coalesce(
        any_value(abk.total_bookings_last_30d), 0
    ) as artist_total_bookings_main_category_last_30d,
    coalesce(
        any_value(aoc.total_consultations_last_30d), 0
    ) as artist_total_offer_consultations_main_category_last_30d
from {{ ref("int_applicative__artist") }} as a
left join artist_product_offers as apo on a.artist_id = apo.artist_id
left join artist_consultations as ac on a.artist_id = ac.artist_id
left join
    artist_main_search_group_name_and_bookable_flag as amc
    on a.artist_id = amc.artist_id
left join
    artist_associated_search_group_names_array as aac on a.artist_id = aac.artist_id
left join
    artist_offer_consultation_counts_last_30d_per_search_group_name as aoc
    on a.artist_id = aoc.artist_id
    and amc.main_search_group_name = aoc.search_group_name
left join
    artist_booking_counts_last_30d_per_search_group_name as abk
    on a.artist_id = abk.artist_id
    and amc.main_search_group_name = abk.search_group_name
group by
    a.artist_id,
    a.artist_name,
    a.normalized_artist_name,
    a.artist_description,
    a.artist_biography,
    a.artist_mediation_uuid,
    a.artist_computed_image,
    a.wikidata_id,
    a.wikipedia_url,
    a.wikidata_image_file_url,
    a.wikidata_image_license,
    a.wikidata_image_license_url,
    a.wikidata_image_author,
    a.artist_app_search_score,
    a.artist_pro_search_score,
    a.creation_date,
    a.modification_date,
    amc.main_search_group_name,
    amc.main_search_group_name_has_bookable_offer
