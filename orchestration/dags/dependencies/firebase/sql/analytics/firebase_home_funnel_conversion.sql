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
    user_pseudo_id
  , user_id
  , session_id
  , entry_id
  , home_ref.title as entry_name
  , module_id as parent_module_id
  , ref.title as parent_module_name
  , ref.content_type as parent_module_type
  , event_date as module_displayed_date
  , event_timestamp as module_displayed_timestamp
  FROM `{{ bigquery_analytics_dataset }}.firebase_events` events
  LEFT JOIN `{{ bigquery_analytics_dataset }}.contentful_entries` as ref
  on events.module_id = ref.id
  JOIN home_ref
  on events.entry_id = home_ref.id
  WHERE event_name = 'ModuleDisplayedOnHomePage'
  and user_pseudo_id is not null
  and session_id is not null
  {% if params.dag_type == 'intraday' %}
  AND event_date = DATE('{{ ds }}')        
  {% else %}
  AND event_date = DATE('{{ add_days(ds, -1) }}')
  {% endif %}
)
, clicked as (
    SELECT
      user_pseudo_id
      , user_id
      , session_id
      , entry_id
      , home_ref.title as entry_name
      , event_name as click_type
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
    WHERE event_name in ("ExclusivityBlockClicked", "CategoryBlockClicked", "HighlightBlockClicked")
    -- entry_id param is missing for event HighlightBlockClicked because it is available in a prior version of the app. 
    and user_pseudo_id is not null
    and session_id is not null
)
, consultations as (
    WITH relationships as (
        SELECT DISTINCT 
            parent as home_id
            , child as playlist_id
            , e.title as playlist_name
        FROM `{{ bigquery_analytics_dataset }}.contentful_relationships` r
        LEFT JOIN `{{ bigquery_analytics_dataset }}.contentful_entries` e
        ON r.child = e.id
    ),
    offer as (
    SELECT 
      user_pseudo_id
      , user_id
      , session_id
      , entry_id
      , module_id
      , origin
      , events.offer_id
      , events.venue_id
      , event_timestamp as consult_offer_timestamp
    FROM `{{ bigquery_analytics_dataset }}.firebase_events` events
    WHERE event_name = 'ConsultOffer'
    AND origin in ("home", "exclusivity", "venue")
    AND user_pseudo_id is not null
    QUALIFY rank() over(partition by user_pseudo_id, session_id, offer_id order by event_timestamp desc) = 1 -- get the last consultation
    ),
    venue as ( -- get the module_id for venue playlist
      SELECT
        user_pseudo_id
        , user_id
        , session_id
        , entry_id
        , module_id
        , offer_id
        , venue_id
        , event_timestamp as consult_venue_timestamp
      FROM `{{ bigquery_analytics_dataset }}.firebase_events`
      WHERE event_name = "ConsultVenue"
      AND origin = "home" 
      and user_pseudo_id is not null
      QUALIFY rank() over(partition by user_pseudo_id, session_id, venue_id order by event_timestamp desc) = 1 -- get the last consultation
  )
  SELECT 
      offer.user_pseudo_id
      , offer.user_id
      , offer.session_id
      , coalesce(offer.entry_id, venue.entry_id) as entry_id
      , home_ref.title as entry_name
      , coalesce(offer.module_id, venue.module_id) as module_id
      , ref.title as module_name
      , ref.content_type
      , offer.origin
      , offer.offer_id
      , venue.venue_id
      , consult_offer_timestamp
      , consult_venue_timestamp
      , home_id
      , playlist_id
      , playlist_name
    FROM venue
    FULL OUTER JOIN offer
    ON offer.user_pseudo_id = venue.user_pseudo_id
    AND offer.session_id = venue.session_id
    AND offer.venue_id = venue.venue_id
    AND offer.consult_offer_timestamp >= venue.consult_venue_timestamp
    LEFT JOIN home_ref
    ON coalesce(offer.entry_id, venue.entry_id) = home_ref.id
    LEFT JOIN `{{ bigquery_analytics_dataset }}.contentful_entries` as ref
    ON ref.id =  coalesce(offer.module_id, venue.module_id)
    JOIN relationships -- inner join to get only known relationships between playlist and homepages.
    ON relationships.playlist_id =  coalesce(offer.module_id, venue.module_id)
    AND relationships.home_id = coalesce(offer.entry_id, venue.entry_id)
)

SELECT 
    displayed.user_pseudo_id
  , displayed.user_id
  , displayed.session_id
  , displayed.entry_id -- first touch
  , displayed.entry_name
  , displayed.parent_module_id -- Can be category list block, highlight etc / second touch
  , displayed.parent_module_name
  , displayed.parent_module_type
  , destination_entry_id --  2nd home id in case of redirection to an home_id
  , destination_entry_name
  , click_type
  , clicked.module_id as child_module_id -- category block id
  , clicked.module_name as child_module_name
  , playlist_id 
  , playlist_name
  , origin
  , content_type
  , consultations.offer_id
  , consultations.venue_id
  , booking_id
  , module_displayed_date
  , module_displayed_timestamp
  , module_clicked_timestamp
  , consult_venue_timestamp
  , consult_offer_timestamp
  , booking_timestamp
FROM displayed
LEFT JOIN clicked
  ON displayed.user_pseudo_id = clicked.user_pseudo_id 
  AND displayed.session_id = clicked.session_id
  AND displayed.entry_id = clicked.entry_id
  AND displayed.parent_module_id = coalesce(clicked.module_list_id, clicked.module_id) -- coalesce pour ne pas exclure les blocs qui ne redirigent pas vers une home
  AND displayed.module_displayed_timestamp <= clicked.module_clicked_timestamp
LEFT JOIN consultations
  ON displayed.user_pseudo_id = consultations.user_pseudo_id
  AND displayed.session_id =  consultations.session_id
  AND coalesce(clicked.destination_entry_id, displayed.entry_id) = consultations.home_id -- coalesce pour ne pas exclure les consultations d'offres "directes"
  AND coalesce(clicked.module_clicked_timestamp, displayed.module_displayed_timestamp) <= consultations.consult_offer_timestamp
LEFT JOIN `{{ bigquery_analytics_dataset }}.firebase_bookings` as bookings
  ON displayed.user_pseudo_id = bookings.user_pseudo_id
  AND displayed.session_id = bookings.session_id
  AND consultations.offer_id = bookings.offer_id
  AND consultations.consult_offer_timestamp <= bookings.booking_timestamp