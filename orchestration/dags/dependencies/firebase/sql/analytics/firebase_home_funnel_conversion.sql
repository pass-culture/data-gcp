with home_ref as (
    SELECT *
    from `{{ bigquery_analytics_dataset }}.contentful_entries`
    where content_type = "homepageNatif"
)
, category_block_ref as (
    SELECT *
    from `{{ bigquery_analytics_dataset }}.contentful_entries`
    where content_type = "categoryBlock"
)
, category_list_ref as (  
    SELECT *
    from `{{ bigquery_analytics_dataset }}.contentful_entries`
    where content_type = "categoryList"
)
, displayed as (
  ---- modules displayed
  SELECT 
    user_id
  , session_id
  , entry_id
  , home_ref.title as entry_name
  , module_id
  , ref.title as module_name
  , ref.content_type as module_content_type
  , event_date as module_displayed_date
  , event_timestamp as module_displayed_timestamp
  FROM `{{ bigquery_analytics_dataset }}.firebase_events` events
  LEFT JOIN `{{ bigquery_analytics_dataset }}.contentful_entries` as ref
  on events.module_id = ref.id
  LEFT JOIN home_ref
  on events.entry_id = home_ref.id
  WHERE event_name = 'ModuleDisplayedOnHomePage'
  and user_id is not null
  -- and user_id = "2141130"
  and session_id is not null
  -- and session_id = 1682437822
)
, clicked as (
    SELECT
        user_id
        , session_id
        , entry_id
        , home_ref.title as entry_name
        -- , event_name
        , destination_entry_id
        , home_ref2.title as destination_entry_name
        , module_list_id
        , category_list_ref.title as module_list_name -- category list
        , module_id
        , category_block_ref.title as module_name -- category block
        , event_timestamp as module_clicked_timestamp
    FROM `{{ bigquery_analytics_dataset }}.firebase_events` events
    LEFT JOIN category_list_ref
    ON events.module_list_id = category_list_ref.id
    LEFT JOIN category_block_ref
    ON events.module_id = category_block_ref.id
    LEFT JOIN home_ref
    ON events.entry_id = home_ref.id
    LEFT JOIN home_ref as home_ref2
    ON events.destination_entry_id = home_ref2.id
    -- WHERE event_name in ("BusinessBlockClicked", "ExclusivityBlockClicked", "SeeMoreClicked", "CategoryBlockClicked", "HighlightBlockClicked")
    WHERE event_name = "CategoryBlockClicked"
    -- entry_id param is missing for event HighlightBlockClicked
    and user_id is not null
    -- and user_id = "2141130"
    and session_id is not null
    -- and session_id = 1682437822
)
, consult_offer as (
    WITH relationships as (
        SELECT DISTINCT 
            parent as home_id
            , child as playlist_id
            , e.title as playlist_name
        FROM `{{ bigquery_analytics_dataset }}.contentful_relationships` r
        LEFT JOIN `{{ bigquery_analytics_dataset }}.contentful_entries` e
        ON r.child = e.id
    )
    SELECT 
        user_id
        , session_id
        , entry_id
        , home_ref.title as entry_name
        , module_id
        , ref.title as module_name
        , ref.content_type
        , origin
        , events.offer_id
        , event_timestamp as consult_offer_timestamp
        , home_id
        , playlist_id
        , playlist_name
    FROM `{{ bigquery_analytics_dataset }}.firebase_events` events
    LEFT JOIN home_ref
    ON events.entry_id = home_ref.id
    LEFT JOIN `{{ bigquery_analytics_dataset }}.contentful_entries` as ref
    ON ref.id = events.module_id
    JOIN relationships -- inner join to get only known relationships between playlist and homepages.
    ON relationships.playlist_id = events.module_id
    AND relationships.home_id = events.entry_id
    WHERE event_name = 'ConsultOffer'
    AND origin in  ("home", "exclusivity", "venue")
    AND user_id is not null
    AND module_id is not null
    QUALIFY rank() over(partition by user_id, session_id, offer_id order by event_timestamp desc) = 1 -- get the last consultation
)
, bookings as (
  SELECT 
    user_id
    , session_id
    , offer_id
    , booking_id
    , event_timestamp as booking_timestamp
  FROM `{{ bigquery_analytics_dataset }}.firebase_events`
  WHERE event_name = "BookingConfirmation"
)

SELECT 
    displayed.user_id
  , displayed.session_id
  , displayed.entry_id -- first touch
  , displayed.entry_name
  , displayed.module_id as transitional_module_id -- Can be category list block, highlight etc / second touch
  , displayed.module_name as transitional_module_name 
  , destination_entry_id --  2nd home id in case of redirection to an home_id
  , destination_entry_name
  , clicked.module_id as transitional_block_id -- category block id
  , clicked.module_name as transitional_block_name
  , playlist_id 
  , playlist_name
  , origin
  , content_type
  , consult_offer.offer_id
  , booking_id
  , module_displayed_date
  , module_displayed_timestamp
  , module_clicked_timestamp
  , consult_offer_timestamp
  , booking_timestamp
FROM displayed
LEFT JOIN clicked
  ON displayed.user_id = clicked.user_id 
  AND displayed.session_id = clicked.session_id
  AND displayed.entry_id = clicked.entry_id
  AND displayed.module_id = clicked.module_list_id
  AND displayed.module_displayed_timestamp <= clicked.module_clicked_timestamp
LEFT JOIN consult_offer
  ON displayed.user_id = consult_offer.user_id
  AND displayed.session_id =  consult_offer.session_id
  AND coalesce(clicked.destination_entry_id, displayed.entry_id) = consult_offer.thematic_home_id -- coalesce pour ne pas exclure les consultations d'offres "directes"
  AND coalesce(clicked.module_clicked_timestamp, displayed.module_displayed_timestamp) <= consult_offer.consult_offer_timestamp
LEFT JOIN bookings
  ON displayed.user_id = bookings.user_id
  AND displayed.session_id = bookings.session_id
  AND consult_offer.offer_id = bookings.offer_id
  AND consult_offer.consult_offer_timestamp <= bookings.booking_timestamp
