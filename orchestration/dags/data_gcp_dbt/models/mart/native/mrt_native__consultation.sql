{{
    config(
        **custom_incremental_config(
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'consultation_date', 'data_type': 'date'}
    )
) }}

with discoveries_by_consultation as (
SELECT
    consultation_id,
    MAX(CASE WHEN type = 'item' then 1 else 0 END) AS item_discovery_score,
    MAX(CASE WHEN type = 'offer_subcategory' then 1 else 0 END) AS subcategory_discovery_score,
    MAX(CASE WHEN type = 'offer_category' then 1 else 0 END) AS category_discovery_score,
from {{ ref('int_metric__discovery_score')}}
group by consultation_id
)

SELECT
    consult.consultation_id,
    consult.consultation_date,
    consult.origin,
    consult.offer_id,
    consult.user_id,
    consult.unique_session_id,
    dc.item_discovery_score,
    dc.subcategory_discovery_score,
    dc.category_discovery_score,
    dc.item_discovery_score + dc.subcategory_discovery_score + dc.category_discovery_score as discovery_score,
    case when category_discovery_score > 0 then true else false end as is_category_discovered,
    case when subcategory_discovery_score > 0 then true else false end as is_subcategory_discovered,
    offer.item_id,
    offer.offer_subcategory_id,
    offer.offer_category_id,
    offer.offer_name,
    offer.venue_id,
    offer.venue_name,
    offer.venue_type_label,
    offer.partner_id,
    offer.offerer_id,
    user.user_region_name,
    user.user_department_code,
    user.user_activity,
    user.user_is_priority_public,
    user.user_is_unemployed,
    user.user_is_in_qpv,
    user.user_macro_density_label,
    consult.traffic_medium,
    consult.traffic_campaign,
    consult.module_id
FROM {{ ref('int_firebase__native_consultation')}} AS consult
left join discoveries_by_consultation as dc on dc.consultation_id = consult.consultation_id
left join {{ ref('int_global__offer')}} as offer on consult.offer_id = offer.offer_id
left join {{ ref('int_global__user')}} as user on consult.user_id = user.user_id
{% if is_incremental() %}
where consultation_date >= date_sub('{{ ds() }}', INTERVAL 3 day)
{% else %}
where consultation_date >= date_sub('{{ ds() }}', INTERVAL 1 year)
{% endif %}
{{
    config(
        **custom_incremental_config(
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'consultation_date', 'data_type': 'date'}
    )
) }}

-- Next 4 subqueries : identify the micro-origin of each consultation that comes from a venue page or a similar offer page (origin of venue consultation and origin of inital offer consultation)
WITH consult_venue AS (
    SELECT
        user_id,
        unique_session_id,
        event_timestamp as venue_consultation_timestamp,
        venue_id,
        origin as consult_venue_origin
    FROM {{ ref('int_firebase__native_event') }}
    WHERE event_name = 'ConsultVenue'
        AND unique_session_id IS NOT NULL
        {% if is_incremental() %}
        AND event_date = date_sub('{{ ds() }}', INTERVAL 3 day)
        {% endif %}
),

consult_offer AS (
    SELECT
        user_id,
        unique_session_id,
        consultation_timestamp,
        consultation_id,
        offer_id,
        venue_id,
        similar_offer_id,
        origin as consult_offer_origin
    FROM {{ ref('int_firebase__native_consultation') }}
    WHERE 1=1
        {% if is_incremental() %}
        AND event_date = date_sub('{{ ds() }}', INTERVAL 3 day)
        {% endif %}
),

consult_offer_through_venue AS (
    SELECT
        co.*,
        cv.consult_venue_origin
    FROM consult_offer co
        LEFT JOIN consult_venue cv ON cv.unique_session_id = co.unique_session_id
            AND cv.venue_id = co.venue_id
            AND cv.venue_consultation_timestamp <= co.consultation_timestamp
    WHERE co.consult_offer_origin = "venue"
    QUALIFY row_number() over (partition by co.unique_session_id, co.venue_id, co.offer_id order by co.consultation_timestamp ASC) = 1 -- keep 1st offer consultation after venue consultation
),

consult_offer_through_similar_offer AS (
    SELECT 
        co1.*
        ,co2.consult_offer_origin as consult_similar_offer_origin
    FROM consult_offer co1 
        LEFT JOIN consult_offer co2 ON co1.unique_session_id = co2.unique_session_id
            AND co1.similar_offer_id=co2.offer_id
            AND co2.consultation_timestamp <= co1.consultation_timestamp
    WHERE co1.consult_offer_origin = "similar_offer"
    QUALIFY row_number() over (partition by co1.unique_session_id, co1.venue_id, co1.offer_id order by co1.consultation_timestamp ASC) = 1 -- keep 1st similar offer consultation after offer consultation 
)

SELECT 
    fc.user_id,
    fc.consultation_date,
    fc.consultation_timestamp,
    fc.offer_id,
    o.item_id,
    o.offer_subcategory_id,
    o.offer_category_id,
    fc.origin,
    fc.unique_session_id,
    fc.consultation_id,
    fc.venue_id,
    fc.traffic_medium,
    fc.traffic_campaign,
    fc.entry_id,
    ht.home_name,
    ht.home_audience,
    ht.user_lifecycle_home,
    CASE WHEN fc.origin="similar_offer" AND fc.similar_offer_playlist_type = "sameCategorySimilarOffers" THEN "same_category_similar_offer"
        WHEN fc.origin="similar_offer" AND fc.similar_offer_playlist_type = "otherCategoriesSimilarOffers" THEN "other_category_similar_offer"
        ELSE fc.origin END AS consultation_macro_origin,
    CASE WHEN ht.entry_id IS NOT NULL AND ht.home_type IS NOT NULL THEN CONCAT("home_",home_type)
        WHEN ht.entry_id IS NOT NULL AND ht.home_type IS NULL THEN "home_without_tag"
        WHEN fc.origin="search" AND fc.search_query_input_is_generic IS TRUE THEN "generic_query_search"
        WHEN fc.origin="search" AND fc.search_query_input_is_generic IS FALSE THEN "specific_query_search"
        WHEN fc.origin="search" AND fc.query is NULL THEN "landing_search"
        WHEN fc.origin="venue" AND ov.consult_venue_origin="offer" THEN "offer_venue"
        WHEN fc.origin="venue" AND ov.consult_venue_origin="searchVenuePlaylist" THEN "search_venue_playlist"
        WHEN fc.origin="venue" AND ov.consult_venue_origin="searchAutoComplete" THEN "search_venue_autocomplete"
        WHEN fc.origin="venue" AND ov.consult_venue_origin="venueMap" THEN "venue_map"
        WHEN fc.origin="venue" AND ov.consult_venue_origin IN ("home","venueList") THEN "home_venue_playlist"
        WHEN fc.origin="venue" AND ov.consult_venue_origin="deeplink" THEN "venue_deeplink"
        WHEN fc.origin="offer" AND fc.multi_venue_offer_id IS NOT NULL THEN "multi_venue_offer"
        WHEN fc.origin="similar_offer" THEN CONCAT("similar_offer_",so.consult_similar_offer_origin)
        ELSE fc.origin END as consultation_micro_origin
FROM {{ ref('int_firebase__native_consultation') }} fc
LEFT JOIN {{ ref('int_applicative__offer') }} AS o ON fc.offer_id = o.offer_id
LEFT JOIN {{ ref('int_contentful__home_tag') }} AS ht ON ht.entry_id=fc.entry_id
LEFT JOIN consult_offer_through_venue AS ov ON ov.consultation_id = fc.consultation_id AND fc.origin = "venue"
LEFT JOIN consult_offer_through_similar_offer AS so ON so.consultation_id = fc.consultation_id AND fc.origin = "similar_offer"
WHERE 1=1
    {% if is_incremental() %}
    AND fc.consultation_date = date_sub('{{ ds() }}', INTERVAL 3 day)
    {% endif %}