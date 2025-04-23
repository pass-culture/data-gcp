{{ config(materialized="view") }}

with
    recommendable_offers_data as (
        select
            available_offers.*,
            row_number() over (
                partition by available_offers.offer_id
                order by
                    available_offers.stock_price asc,
                    available_offers.stock_beginning_date asc
            ) as stock_rank
        from
            (
                select
                    item_id,
                    offer_id,
                    product_id,
                    venue_id,
                    venue_latitude,
                    venue_longitude,
                    offer_creation_date,
                    stock_beginning_date,
                    max(stock_price) as stock_price,
                    max(category) as category,
                    max(offer_type_domain) as offer_type_domain,
                    max(offer_type_label) as offer_type_label,
                    max(array_to_string(offer_type_labels, ';')) as offer_type_labels,
                    max(item_count) as total_offers,
                    max(booking_number) as booking_number,
                    max(booking_number_last_7_days) as booking_number_last_7_days,
                    max(booking_number_last_14_days) as booking_number_last_14_days,
                    max(booking_number_last_28_days) as booking_number_last_28_days,
                    max(is_underage_recommendable) as is_underage_recommendable,
                    max(is_sensitive) as is_sensitive,
                    max(is_restrained) as is_restrained,
                    max(subcategory_id) as subcategory_id,
                    max(search_group_name) as search_group_name,
                    max(name) as name,  -- noqa: RF04
                    max(gtl_id) as gtl_id,
                    max(gtl_l1) as gtl_l1,
                    max(gtl_l2) as gtl_l2,
                    max(gtl_l3) as gtl_l3,
                    max(gtl_l4) as gtl_l4,
                    max(topic_id) as topic_id,
                    max(cluster_id) as cluster_id,
                    max(semantic_emb_mean) as semantic_emb_mean,
                    max(is_national) as is_national,
                    max(url is not null) as is_numerical,
                    max(url is null) as is_geolocated,
                    max(offer_is_duo) as offer_is_duo,
                    max(default_max_distance) as default_max_distance
                from {{ ref("ml_reco__available_offer") }}
                group by 1, 2, 3, 4, 5, 6, 7, 8
            ) as available_offers
    ),

    item_embeddings as (
        select item_id, to_json_string(item_embedding) as item_embedding_json
        from {{ ref("ml_feat__two_tower_last_item_embedding") }}
    ),

    offer_features as (
        select
            offer_id,
            is_geolocated as offer_is_geolocated,
            offer_created_delta_in_days as offer_creation_days,
            offer_mean_stock_price as offer_stock_price,
            offer_max_stock_beginning_days as offer_stock_beginning_days,
            offer_centroid_x,
            offer_centroid_y
        from {{ ref("ml_feat__offer_feature") }}
    ),

        select
            ro.item_id,
            ro.offer_id,
            ro.product_id,
            ro.category,
            ro.subcategory_id,
            ro.search_group_name,
            ro.venue_id,
            ro.venue_latitude,
            ro.venue_longitude,
            ro.name,
            ro.gtl_id,
            ro.gtl_l1,
            ro.gtl_l2,
            ro.gtl_l3,
            ro.gtl_l4,
            ro.topic_id,
            ro.cluster_id,
            ro.semantic_emb_mean,
            ro.is_numerical,
            ro.is_national,
            ro.is_geolocated,
            ro.offer_creation_date,
            ro.stock_beginning_date,
            ro.stock_price,
            ro.offer_is_duo,
            ro.offer_type_domain,
            ro.offer_type_label,
            ro.offer_type_labels,
            ro.total_offers,
            ro.booking_number,
            ro.booking_number_last_7_days,
            ro.booking_number_last_14_days,
            ro.booking_number_last_28_days,
            ro.is_underage_recommendable,
            ro.is_sensitive,
            ro.is_restrained,
            ro.default_max_distance,
            item_embeddings.item_embedding_json as new_item_embedding_json,
            offer_features.offer_is_geolocated as new_offer_is_geolocated,
            offer_features.offer_creation_days as new_offer_creation_days,
            offer_features.offer_stock_price as new_offer_stock_price,
            offer_features.offer_stock_beginning_days as new_offer_stock_beginning_days,
            offer_features.offer_centroid_x as new_offer_centroid_x,
            offer_features.offer_centroid_y as new_offer_centroid_y,
            row_number() over () as unique_id

        from recommendable_offers_data as ro
        left join offer_features on ro.offer_id = offer_features.offer_id
        left join item_embeddings on ro.item_id = item_embeddings.item_id
        where ro.stock_rank < 30  -- only next 30 events
