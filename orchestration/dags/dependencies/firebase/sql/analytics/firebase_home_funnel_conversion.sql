with child_home as (
 -- Home qui proviennent d'un category block
  SELECT DISTINCT
    e1.home_entry_id
    , e2.title
  FROM `{{ bigquery_analytics_dataset }}.contentful_entries` e1
  JOIN `{{ bigquery_analytics_dataset }}.contentful_entries` e2
  ON e1.home_entry_id = e2.id
  WHERE e1.content_type = "categoryBlock"
)
, home_ref as (
    SELECT 
      home.id
      , home.title
    from `{{ bigquery_analytics_dataset }}.contentful_entries` home
    LEFT JOIN child_home
    ON home.id = child_home.home_entry_id
    WHERE child_home.home_entry_id is null -- retirer les homes qui sont spécifiques
    AND home.content_type = "homepageNatif"
)
, parents_ref as (  
    SELECT *
    from `{{ bigquery_analytics_dataset }}.contentful_entries`
    where content_type in ("categoryList", "thematicHighlight")
)
, children_ref as (
    SELECT *
    from `{{ bigquery_analytics_dataset }}.contentful_entries`
    where content_type in ("categoryBlock", "thematic_highlight_info")
)
, displayed as (
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
  JOIN home_ref
  on events.entry_id = home_ref.id
  WHERE event_name = 'ModuleDisplayedOnHomePage'
  and user_id is not null
  and session_id is not null
)
, clicked as (
    SELECT
      user_id
      , session_id
      , entry_id
      , home_ref.title as entry_name
      , destination_entry_id
      , e.title as destination_entry_name
      , module_list_id
      , parents_ref.title as module_list_name -- category list / highlight name
      , module_id
      , children_ref.title as module_name -- category block / highlight name
      , event_timestamp as module_clicked_timestamp
    FROM `{{ bigquery_analytics_dataset }}.firebase_events` events
    LEFT JOIN parents_ref
    ON events.module_list_id = parents_ref.id
    LEFT JOIN children_ref
    ON events.module_id = children_ref.id
    LEFT JOIN home_ref
    ON events.entry_id = home_ref.id
    LEFT JOIN `{{ bigquery_analytics_dataset }}.contentful_entries` as e
    ON events.destination_entry_id = e.id
    WHERE event_name in ("CategoryBlockClicked", "HighlightBlockClicked")
    -- entry_id param is missing for event HighlightBlockClicked because it is available in a prior version of the app. 
    and user_id is not null
    and session_id is not null
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

SELECT 
    displayed.user_id
  , displayed.session_id
  , displayed.entry_id -- first touch
  , displayed.entry_name
  , displayed.module_id as parent_module_id -- Can be category list block, highlight etc / second touch
  , displayed.module_name as parent_module_name 
  , destination_entry_id --  2nd home id in case of redirection to an home_id
  , destination_entry_name
  , clicked.module_id as child_module_id -- category block id
  , clicked.module_name as child_module_name
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
  AND coalesce(clicked.destination_entry_id, displayed.entry_id) = consult_offer.home_id -- coalesce pour ne pas exclure les consultations d'offres "directes"
  AND coalesce(clicked.module_clicked_timestamp, displayed.module_displayed_timestamp) <= consult_offer.consult_offer_timestamp
LEFT JOIN `{{ bigquery_analytics_dataset }}.firebase_bookings` as  bookings
  ON displayed.user_id = bookings.user_id
  AND displayed.session_id = bookings.session_id
  AND consult_offer.offer_id = bookings.offer_id
  AND consult_offer.consult_offer_timestamp <= bookings.booking_timestamp
