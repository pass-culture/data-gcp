{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={
                "field": "first_date",
                "data_type": "date",
                "granularity": "day",
            },
            on_schema_change="append_new_columns",
        )
    )
}}

with

    -- Step 1: Extracting Consulted Offers
    consulted_offers as (
        select
            unique_session_id,
            user_id,
            offer_id,
            search_id,
            unique_search_id,
            event_timestamp as consult_timestamp,
            event_date as consult_date
        from {{ ref("int_firebase__native_event") }}
        where
            event_name = 'ConsultOffer' and search_id is not null
            {% if is_incremental() %}
                and event_date
                between date_sub(date('{{ ds() }}'), interval 3 day) and date(
                    '{{ ds() }}'
                )
            {% endif %}
        qualify
            row_number() over (
                partition by unique_session_id, offer_id, unique_search_id
                order by event_timestamp
            )
            = 1
    ),

    -- Step 2: Extracting Booked Offers
    booked_offers as (
        select
            co.unique_session_id,
            co.user_id,
            co.search_id,
            co.unique_search_id,
            co.offer_id,
            co.consult_timestamp,
            booking.diversity_score as delta_diversification
        from consulted_offers as co
        inner join
            {{ ref("int_firebase__native_event") }} as ne
            on co.unique_session_id = ne.unique_session_id
            and co.offer_id = ne.offer_id
            and ne.event_name = 'BookingConfirmation'
            and ne.event_timestamp > co.consult_timestamp
            {% if is_incremental() %}
                and ne.event_date
                between date_sub(date('{{ ds() }}'), interval 3 day) and date(
                    '{{ ds() }}'
                )
            {% endif %}
        left join
            {{ ref("mrt_global__booking") }} as booking
            on co.user_id = booking.user_id
            and co.offer_id = booking.offer_id
            and date(co.consult_timestamp) = date(booking.booking_creation_date)
        qualify
            row_number() over (
                partition by co.unique_search_id, co.unique_session_id, co.offer_id
                order by ne.event_timestamp
            )
            = 1
    ),

    -- Step 3: Aggregating Bookings by Search ID
    bookings_aggregated as (
        select distinct
            search_id,
            unique_search_id,
            unique_session_id,
            user_id,
            count(distinct offer_id) over (
                partition by unique_search_id, unique_session_id
            ) as nb_offers_booked,
            sum(delta_diversification) over (
                partition by unique_search_id, unique_session_id
            ) as total_diversification
        from booked_offers
    ),

    -- Step 4: Identifying the First Search Performed
    first_search as (
        select
            search_id,
            unique_session_id,
            unique_search_id,
            case
                when query is not null
                then 'text_input'
                when search_categories_filter is not null
                then 'categories_filter'
                when search_genre_types_filter is not null
                then 'genre_types_filter'
                when search_native_categories_filter is not null
                then 'native_categories_filter'
                when search_date_filter is not null
                then 'date_filter'
                when search_max_price_filter is not null
                then 'max_price_filter'
                when search_location_filter is not null
                then 'location_filter'
                when search_accessibility_filter is not null
                then 'accessibility_filter'
                else 'Autre'
            end as first_filter_applied
        from {{ ref("int_firebase__native_event") }}
        where
            event_name = 'PerformSearch' and unique_search_id is not null
            {% if is_incremental() %}
                and event_date
                between date_sub(date('{{ ds() }}'), interval 3 day) and date(
                    '{{ ds() }}'
                )
            {% endif %}
            and not (
                event_name = 'PerformSearch'
                and search_type = 'Suggestions'
                and query is null
                and search_categories_filter is null
                and search_native_categories_filter is null
                and search_location_filter = '{"locationType":"EVERYWHERE"}'
            )
        qualify
            row_number() over (
                partition by unique_search_id, unique_session_id
                order by event_timestamp
            )
            = 1
    ),

    -- Step 5: Identifying the Last Search Performed
    last_search as (
        select
            unique_search_id,
            unique_session_id,
            event_date as first_date,
            event_timestamp as first_timestamp,
            app_version,
            query as query_input,
            search_type,
            search_date_filter,
            search_location_filter,
            search_categories_filter,
            search_genre_types_filter,
            search_max_price_filter,
            search_is_autocomplete,
            search_is_based_on_history,
            search_offer_is_duo_filter,
            search_native_categories_filter,
            search_accessibility_filter,
            user_location_type,
            search_query_input_is_generic
        from {{ ref("int_firebase__native_event") }}
        where
            event_name = 'PerformSearch'
            {% if is_incremental() %}
                and event_date
                between date_sub(date('{{ ds() }}'), interval 3 day) and date(
                    '{{ ds() }}'
                )
            {% endif %}
            and not (
                event_name = 'PerformSearch'
                and search_type = 'Suggestions'
                and query is null
                and search_categories_filter is null
                and search_native_categories_filter is null
                and search_location_filter = '{"locationType":"EVERYWHERE"}'
            )
        qualify
            row_number() over (
                partition by unique_search_id, unique_session_id
                order by event_timestamp desc
            )
            = 1
    ),

    -- Step 6: Conversion Metrics per Search
    conversion_metrics as (
        select
            ls.unique_session_id,
            ls.unique_search_id,
            count(
                distinct case when ne.event_name = 'ConsultOffer' then ne.offer_id end
            ) as nb_offers_consulted,
            count(
                distinct case
                    when ne.event_name = 'HasAddedOfferToFavorites' then ne.offer_id
                end
            ) as nb_offers_added_to_favorites,
            count(
                case when ne.event_name = 'NoSearchResult' then 1 end
            ) as nb_no_search_result,
            count(
                case when ne.event_name = 'PerformSearch' then 1 end
            ) as nb_iterations_search,
            count(
                case
                    when ne.event_name = 'VenuePlaylistDisplayedOnSearchResults' then 1
                end
            ) as nb_venue_playlist_displayed_on_search_results,
            count(
                distinct case when ne.event_name = 'ConsultVenue' then ne.venue_id end
            ) as nb_venues_consulted,
            count(
                case
                    when ne.event_name = 'ConsultArtist' and ne.origin = 'search' then 1
                end
            ) as nb_artists_consulted,
            max(
                coalesce(ne.event_name = 'ExtendSearchRadiusClicked', false)
            ) as has_extended_search_radius
        from last_search as ls
        left join
            {{ ref("int_firebase__native_event") }} as ne
            on ne.event_name in (
                'NoSearchResult',
                'ConsultOffer',
                'HasAddedOfferToFavorites',
                'VenuePlaylistDisplayedOnSearchResults',
                'ConsultVenue',
                'ExtendSearchRadiusClicked',
                'ConsultArtist'
            )
            and ls.unique_session_id = ne.unique_session_id
            and ls.unique_search_id = ne.unique_search_id
            and ls.first_date = ne.event_date
        group by ls.unique_session_id, ls.unique_search_id
    ),

    -- Step 7: Aggregating All Search Data
    agg_search_data as (
        select
            ls.*,
            cm.* except (unique_search_id, unique_session_id),
            fs.first_filter_applied
        from last_search as ls
        inner join first_search as fs using (unique_search_id, unique_session_id)
        inner join conversion_metrics as cm using (unique_search_id, unique_session_id)
    ),

    -- Step 8: Categorizing Search Type and Calculating Additional Metrics
    final_data as (
        select
            asd.*,
            bpsi.nb_offers_booked,
            bpsi.total_diversification,
            bpsi.user_id,
            lead(asd.first_date) over (
                partition by bpsi.unique_session_id order by asd.first_timestamp
            )
            is not null as made_another_search
        from agg_search_data as asd
        left join
            bookings_aggregated as bpsi using (unique_search_id, unique_session_id)
    )

select
    unique_search_id,
    unique_session_id,
    user_id,
    first_date,
    first_timestamp,
    app_version,
    query_input,
    search_type,
    search_date_filter,
    search_location_filter,
    search_categories_filter,
    search_genre_types_filter,
    search_max_price_filter,
    search_is_autocomplete,
    search_is_based_on_history,
    search_offer_is_duo_filter,
    search_native_categories_filter,
    search_accessibility_filter,
    user_location_type,
    first_filter_applied,
    nb_offers_consulted,
    nb_offers_added_to_favorites,
    nb_no_search_result,
    nb_iterations_search,
    nb_venue_playlist_displayed_on_search_results,
    nb_venues_consulted,
    nb_artists_consulted,
    has_extended_search_radius,
    made_another_search,
    search_query_input_is_generic,
    nb_offers_booked,
    total_diversification,
    case
        when query_input is not null and not search_query_input_is_generic
        then 'specific_search'
        else 'discovery_search'
    end as search_objective
from final_data
