{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'event_date', 'data_type': 'date'},
        on_schema_change = "sync_all_columns"
    )
}}

WITH mapping_module_name_and_id AS (
    SELECT
        *
    FROM
        (
            SELECT
                relations.parent as module_id,
                entries.title as module_name,
                row_number() over (
                    partition by entries.title
                    order by
                        updated_at DESC
                ) as rnk
            from
                {{ ref("int_contentful__relationship") }} relations
                inner join {{ ref("int_contentful__entry") }} entries on entries.id = relations.child
            WHERE
                entries.content_type in (
                    "displayParameters",
                    "venuesSearchParameters",
                    "exclusivityDisplayParameters",
                    "business",
                    "category_bloc",
                    "category_list"
                )
        )
    where
        rnk = 1
),
firebase_events AS (
    SELECT
        coalesce(e.module_id, c_name.module_id) as module_id,
        e.*
    EXCEPT
        (module_id, module_name)
    FROM {{ ref('int_firebase__native_event') }} e
        LEFT JOIN mapping_module_name_and_id c_name on c_name.module_name = e.module_name
    WHERE
        event_name in (
            'ConsultOffer',
            "ConsultVenue",
            'BusinessBlockClicked',
            'ExclusivityBlockClicked',
            "SeeMoreClicked",
            'ModuleDisplayedOnHomePage',
            "CategoryBlockClicked"
        )
        AND (
            origin = 'home'
            OR origin = 'exclusivity'
            OR origin IS NULL
        )
        AND event_date >= DATE_SUB('{{ ds() }}', interval 3 day)
        AND event_date <= DATE_ADD('{{ ds() }}', interval 3 day)
),
firebase_module_events AS (
    SELECT
        e.event_date,
        e.event_timestamp,
        -- user
        e.session_id,
        e.user_id,
        e.user_pseudo_id,
        e.unique_session_id,
        e.platform,
        --events
        e.event_name,
        e.offer_id,
        e.booking_id,
        CASE WHEN entries.content_type = "recommendation" THEN e.reco_call_id ELSE NULL END AS call_id,
        -- modules
        entries.title as module_name,
        entries.content_type,
        e.module_id,
        -- take last seen home_id
        COALESCE(
            e.entry_id,
            LAST_VALUE(e.entry_id IGNORE NULLS) OVER (
                PARTITION BY user_id,
                session_id
                ORDER BY
                    event_timestamp RANGE BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ),
            FIRST_VALUE(e.entry_id IGNORE NULLS) OVER (
                PARTITION BY user_id,
                session_id
                ORDER BY
                    event_timestamp RANGE BETWEEN CURRENT ROW
                    AND UNBOUNDED FOLLOWING
            ),
            LAST_VALUE(e.entry_id IGNORE NULLS) OVER (
                PARTITION BY module_id
                ORDER BY
                    event_date RANGE BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            )
        ) AS home_id,
        -- take last seen module_id
        COALESCE(
            module_index,
            LAST_VALUE(module_index IGNORE NULLS) OVER (
                PARTITION BY user_id,
                session_id,
                module_id
                ORDER BY
                    event_timestamp RANGE BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            ),
            FIRST_VALUE(module_index IGNORE NULLS) OVER (
                PARTITION BY user_id,
                session_id,
                module_id
                ORDER BY
                    event_timestamp RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
            ),
            LAST_VALUE(module_index IGNORE NULLS) OVER (
                PARTITION BY module_id
                ORDER BY
                    event_date RANGE BETWEEN UNBOUNDED PRECEDING
                    AND CURRENT ROW
            )
        ) AS module_index
    FROM
        firebase_events e
        LEFT JOIN  {{ ref("int_contentful__entry") }} entries on entries.id = e.module_id
),
-- conversion can be Booking or Favorite
firebase_conversion_step AS (
    SELECT
        conv.event_date,
        conv.event_timestamp,
        conv.session_id,
        conv.user_id,
        conv.user_pseudo_id,
        conv.unique_session_id,
        conv.platform,
        --events
        conv.event_name,
        conv.offer_id,
        conv.booking_id,
        event.call_id,
        event.module_name,
        event.content_type,
        event.module_id,
        event.home_id,
        event.module_index,
        -- take last click event
        ROW_NUMBER() OVER (
            PARTITION BY conv.session_id,
            conv.user_id,
            conv.offer_id,
            conv.event_name
            ORDER BY
                event.event_timestamp DESC
        ) as rank
    FROM {{ ref('int_firebase__native_event') }} conv
        INNER JOIN firebase_module_events event on event.unique_session_id = conv.unique_session_id
        AND event.offer_id = conv.offer_id
        AND event.user_id = conv.user_id -- conversion event after click event
    WHERE
        conv.event_name IN (
            'BookingConfirmation',
            'HasAddedOfferToFavorites'
        )
        AND conv.event_timestamp > event.event_timestamp
        AND conv.event_date >= DATE_SUB(date('{{ ds() }}'), interval 3 day)
        AND conv.event_date <= DATE_ADD(date('{{ ds() }}'), interval 3 day)
),
event_union AS (
    SELECT
        *
    FROM
        firebase_module_events
    UNION
    ALL
    SELECT
        *
    EXCEPT
        (rank)
    FROM
        firebase_conversion_step
    where
        rank = 1 -- only one conversion step per event_name
)

SELECT
    e.event_date,
    e.event_timestamp,
    e.session_id,
    e.user_id,
    e.user_pseudo_id,
    e.unique_session_id,
    e.call_id,
    e.platform,
    e.event_name,
    CASE
        WHEN event_name = "ModuleDisplayedOnHomePage" THEN "display"
        WHEN event_name in (
            "ConsultOffer",
            "ConsultVenue",
            "BusinessBlockClicked",
            "ExclusivityBlockClicked"
        ) THEN "click"
        WHEN event_name in ("SeeMoreClicked") THEN "see_more_click"
        WHEN event_name = "HasAddedOfferToFavorites" THEN "favorite"
        WHEN event_name = "BookingConfirmation" THEN "booking"
    END as event_type,
    e.offer_id,
    e.booking_id,
    e.module_name,
    e.content_type,
    e.module_id,
    e.home_id,
    e.module_index
FROM
    event_union e
    
{% if is_incremental() %}
-- recalculate latest day's data + previous
where date(event_date) BETWEEN date_sub(DATE('{{ ds() }}'), INTERVAL 1 DAY) and DATE('{{ ds() }}')
{% endif %}