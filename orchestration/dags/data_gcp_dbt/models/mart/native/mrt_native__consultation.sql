{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "consultation_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
        )
    )
}}

with
    discoveries_by_consultation as (
        select
            consultation_id,
            max(case when type = 'item' then 1 else 0 end) as item_discovery_score,
            max(
                case when type = 'offer_subcategory' then 1 else 0 end
            ) as subcategory_discovery_score,
            max(
                case when type = 'offer_category' then 1 else 0 end
            ) as category_discovery_score,
        from {{ ref("int_metric__discovery_score") }}
        group by consultation_id
    ),

    -- Next 4 subqueries : identify the micro-origin of each consultation that comes
    -- from a venue page or a similar offer page (origin of venue consultation and
    -- origin of inital offer consultation)
    consult_venue as (
        select
            user_id,
            unique_session_id,
            event_timestamp as venue_consultation_timestamp,
            venue_id,
            origin as consult_venue_origin
        from {{ ref("int_firebase__native_event") }}
        where
            event_name = 'ConsultVenue'
            {% if is_incremental() %}
                and event_date = date_sub('{{ ds() }}', interval 3 day)
            {% endif %}
    ),

    consult_offer as (
        select
            nc.user_id,
            nc.unique_session_id,
            nc.consultation_timestamp,
            nc.consultation_id,
            nc.offer_id,
            go.venue_id,
            nc.similar_offer_id,
            nc.origin as consult_offer_origin
        from {{ ref("int_firebase__native_consultation") }} as nc
        left join {{ ref("int_global__offer") }} as go on nc.offer_id = go.offer_id
        where
            1 = 1
            {% if is_incremental() %}
                and consultation_date = date_sub('{{ ds() }}', interval 3 day)
            {% endif %}
    ),

    consult_offer_through_venue as (
        select co.*, cv.consult_venue_origin
        from consult_offer co
        left join
            consult_venue cv
            and cv.venue_id = co.venue_id
            and cv.venue_consultation_timestamp <= co.consultation_timestamp
            and date(cv.venue_consultation_timestamp) = date(co.consultation_timestamp)
        where co.consult_offer_origin = "venue"
    ),

    consult_offer_through_similar_offer as (
        select co1.*, co2.consult_offer_origin as consult_similar_offer_origin
        from consult_offer co1
        left join
            consult_offer co2
            and co1.similar_offer_id = co2.offer_id
            and co2.consultation_timestamp <= co1.consultation_timestamp
            and date(co1.consultation_timestamp) = date(co2.consultation_timestamp)
        where co1.consult_offer_origin = "similar_offer"
    )

select
    consult.consultation_id,
    consult.consultation_date,
    consult.origin,
    consult.offer_id,
    consult.user_id,
    consult.unique_session_id,
    dc.item_discovery_score,
    dc.subcategory_discovery_score,
    dc.category_discovery_score,
    dc.item_discovery_score
    + dc.subcategory_discovery_score
    + dc.category_discovery_score as discovery_score,
    case
        when category_discovery_score > 0 then true else false
    end as is_category_discovered,
    case
        when subcategory_discovery_score > 0 then true else false
    end as is_subcategory_discovered,
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
    user.user_is_in_education,
    user.user_is_in_qpv,
    user.user_macro_density_label,
    consult.traffic_medium,
    consult.traffic_campaign,
    consult.module_id,
    consult.entry_id,
    ht.home_name,
    ht.home_audience,
    ht.user_lifecycle_home,
    case
        when
            consult.origin = "similar_offer"
            and consult.similar_offer_playlist_type = "sameCategorySimilarOffers"
        then "same_category_similar_offer"
        when
            consult.origin = "similar_offer"
            and consult.similar_offer_playlist_type = "otherCategoriesSimilarOffers"
        then "other_category_similar_offer"
        when consult.origin in ("search", "searchresults", "searchn1", "thematicsearch")
        then "search"
        when consult.origin in ("venueMap", "venuemap")
        then "venue_map"
        when consult.origin in ("video", "videoModal", "video_carousel_block")
        then "video"
        else consult.origin
    end as consultation_macro_origin,
    case
        when consult.origin = "offer" and consult.multi_venue_offer_id is not null
        then "multi_venue_offer"
        when
            concat("home_", ht.home_type)
            in ("home_evenement, n_1", "home_n_1, evenement")
        then "home_evenement_permanent"
        when ht.entry_id is not null and ht.home_type is not null
        then concat("home_", ht.home_type)
        when
            (ht.entry_id is not null or consult.origin = "home")
            and ht.home_type is null
        then "home_without_tag"
        when
            consult.origin in ("search", "searchresults")
            and consult.query is not null
            and consult.search_query_input_is_generic is true
        then "generic_query_search"
        when
            consult.origin in ("search", "searchresults")
            and consult.query is not null
            and consult.search_query_input_is_generic is false
        then "specific_query_search"
        when
            consult.origin in ("search", "searchresults", "searchn1", "thematicsearch")
            and consult.query is null
        then "landing_search"
        when consult.origin = "venue" and ov.consult_venue_origin is not null
        then concat("venue_from_", ov.consult_venue_origin)
        when
            consult.origin = "similar_offer"
            and so.consult_similar_offer_origin is not null
        then concat("similar_offer_from_", so.consult_similar_offer_origin)
        when consult.origin in ("venueMap", "venuemap")
        then "venue_map"
        else consult.origin
    end as consultation_micro_origin
from {{ ref("int_firebase__native_consultation") }} as consult
left join
    discoveries_by_consultation as dc on dc.consultation_id = consult.consultation_id
left join {{ ref("int_global__offer") }} as offer on consult.offer_id = offer.offer_id
left join {{ ref("int_global__user") }} as user on consult.user_id = user.user_id
left join {{ ref("int_contentful__home_tag") }} as ht on ht.entry_id = consult.entry_id
left join
    consult_offer_through_venue as ov
    on ov.consultation_id = consult.consultation_id
    and consult.origin = "venue"
left join
    consult_offer_through_similar_offer as so
    on so.consultation_id = consult.consultation_id
    and consult.origin = "similar_offer"

{% if is_incremental() %}
    where consultation_date >= date_sub('{{ ds() }}', interval 3 day)
{% else %} where consultation_date >= date_sub('{{ ds() }}', interval 1 year)
{% endif %}
