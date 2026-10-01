with
    item_count as (
        select offer.item_id, count(distinct offer.offer_id) as total_offers
        from {{ ref("mrt_global__offer") }} as offer
        group by offer.item_id
    ),

    embeddings as (
        select raw_embeddings.item_id, raw_embeddings.semantic_content_embedding
        from {{ ref("ml_feat__item_embedding") }} as raw_embeddings
    ),

    avg_embedding as (
        select embeddings.item_id, avg(cast(e as float64)) as avg_semantic_embedding
        from
            embeddings,
            unnest(
                split(
                    substr(
                        embeddings.semantic_content_embedding,
                        2,
                        length(embeddings.semantic_content_embedding) - 2
                    )
                )
            ) as e
        group by embeddings.item_id
    ),

    booking_numbers as (
        select
            offer.item_id,
            sum(
                if(
                    booking.booking_creation_date
                    >= date_sub(current_date(), interval 7 day),
                    1,
                    0
                )
            ) as booking_number_last_7_days,
            sum(
                if(
                    booking.booking_creation_date
                    >= date_sub(current_date(), interval 14 day),
                    1,
                    0
                )
            ) as booking_number_last_14_days,
            sum(
                if(
                    booking.booking_creation_date
                    >= date_sub(current_date(), interval 28 day),
                    1,
                    0
                )
            ) as booking_number_last_28_days
        from {{ ref("mrt_global__booking") }} as booking
        inner join
            {{ ref("mrt_global__stock") }} as stock on booking.stock_id = stock.stock_id
        inner join
            {{ ref("mrt_global__offer") }} as offer on stock.offer_id = offer.offer_id
        where
            booking.booking_creation_date >= date_sub(current_date(), interval 28 day)
            and not booking.booking_is_cancelled
        group by offer.item_id
    )

select
    ic.item_id,
    ic.total_offers,
    ae.avg_semantic_embedding,
    bn.booking_number_last_7_days,
    bn.booking_number_last_14_days,
    bn.booking_number_last_28_days
from item_count as ic
left join avg_embedding as ae on ic.item_id = ae.item_id
left join booking_numbers as bn on ic.item_id = bn.item_id
