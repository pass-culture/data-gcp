with
    item_count as (
        select item_id, count(distinct offer_id) as total_offers
        from {{ ref("int_applicative__offer") }}
        group by item_id
    ),

    embeddings as (
        select item_id, semantic_content_embedding as semantic_content_embedding
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
            ) e
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
        from {{ ref("int_applicative__booking") }} booking
        inner join
            {{ ref("int_applicative__stock") }} stock
            on booking.stock_id = stock.stock_id
        inner join
            {{ ref("int_applicative__offer") }} offer on stock.offer_id = offer.offer_id
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
        from {{ source("ml_preproc", "default_item_cluster") }} ic
        left join
            {{ source("ml_preproc", "unconstrained_item_cluster") }} it
            on it.item_id = ic.item_id
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
from item_count ic
left join avg_embedding ae on ae.item_id = ic.item_id
left join booking_numbers bn on bn.item_id = ic.item_id
left join item_clusters icc on ic.item_id = icc.item_id
