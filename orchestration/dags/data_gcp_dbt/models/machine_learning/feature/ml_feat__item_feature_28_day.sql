with -- TODO : move from int_applicative to mrt_global
    item_count as (
        select item_id, count(distinct offer_id) as total_offers
        from {{ ref("mrt_global__offer") }}
        group by item_id
    ),

    embeddings as (
        select item_id, semantic_content_embedding
        from {{ ref("ml_feat__item_embedding") }}
    ),

    avg_embedding as (
        select item_id, avg(cast(e as float64)) as avg_semantic_embedding
        from
            embeddings,
            unnest(
                split(
                    substr(
                        semantic_content_embedding,
                        2,
                        length(semantic_content_embedding) - 2
                    )
                )
            ) as e
        group by item_id
    ),

    booking_numbers as (
        select
            item_id,
            sum(
                if(
                    booking_creation_date >= date_sub(current_date(), interval 7 day),
                    1,
                    0
                )
            ) as booking_number_last_7_days,
            sum(
                if(
                    booking_creation_date >= date_sub(current_date(), interval 14 day),
                    1,
                    0
                )
            ) as booking_number_last_14_days,
            sum(
                if(
                    booking_creation_date >= date_sub(current_date(), interval 28 day),
                    1,
                    0
                )
            ) as booking_number_last_28_days
        from {{ ref("mrt_global__booking") }} as booking
        inner join
            {{ ref("mrt_global__stock") }} as stock
            on booking.stock_id = stock.stock_id
        inner join
            {{ ref("mrt_global__offer") }} as offer on stock.offer_id = offer.offer_id
        where
            booking_creation_date >= date_sub(current_date(), interval 28 day)
            and not booking_is_cancelled
        group by item_id
    ),

    item_clusters as (
        select
            ic.item_id,
            any_value(ic.semantic_cluster_id) as cluster_id,
            any_value(it.semantic_cluster_id) as topic_id  -- TODO: temporary solution, should be removed after the refactor of topics logics.
        from {{ source("ml_preproc", "default_item_cluster") }} as ic
        left join
            {{ source("ml_preproc", "unconstrained_item_cluster") }} as it
            on ic.item_id = it.item_id
        group by 1
    )

select
    ic.item_id,
    ic.total_offers,
    ae.avg_semantic_embedding,
    bn.booking_number_last_7_days,
    bn.booking_number_last_14_days,
    bn.booking_number_last_28_days,
    icc.cluster_id,
    icc.topic_id
from item_count as ic
left join avg_embedding as ae on ic.item_id = ae.item_id
left join booking_numbers as bn on ic.item_id = bn.item_id
left join item_clusters as icc on ic.item_id = icc.item_id
