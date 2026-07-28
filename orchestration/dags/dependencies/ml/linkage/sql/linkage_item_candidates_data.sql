with
    import_embeddings as (
        select
            ie.item_id,
            array(
                select val
                from unnest(ie.semantic_content) as val
                with
                offset pos
                where pos < 128
            ) as embedding
        from `{{ bigquery_ml_feat_dataset }}.item_embedding_refactor` ie
        inner join
            `{{ bigquery_analytics_dataset }}.global_offer` go
            on go.item_id = ie.item_id
        where go.offer_product_id is null
    ),
    offers as (
        select
            go.offer_id,
            go.item_id,
            go.offer_name,
            go.offer_description,
            go.performer,
            go.offer_subcategory_id
        from `{{ bigquery_analytics_dataset }}.global_offer` go
        where go.offer_product_id is null
    ),
    bookings as (
        select bo.offer_id, sum(bo.booking_quantity) as booking_count
        from offers o
        join
            `{{ bigquery_analytics_dataset }}.global_booking` bo
            on bo.offer_id = o.offer_id
        group by 1
    ),
    candidates as (
        select
            case
                when o.item_id like 'link-%'
                then concat('offer-', o.offer_id)
                else o.item_id
            end as item_id,
            import_embeddings.embedding,
            o.offer_name,
            o.offer_description,
            o.performer,
            o.offer_subcategory_id,
            b.booking_count
        from offers o
        inner join import_embeddings on import_embeddings.item_id = o.item_id
        left join bookings b on b.offer_id = o.offer_id
    )
select *
from candidates
qualify row_number() over (partition by item_id order by performer desc) = 1
order by rand()
