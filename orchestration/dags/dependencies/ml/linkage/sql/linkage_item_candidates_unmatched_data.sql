with
    k as (
        select ie.item_id, ie.name_embedding
        from `{{ bigquery_ml_preproc_dataset }}.item_embedding_reduced_32` ie
        inner join
            `{{ bigquery_analytics_dataset }}.global_offer` go
            on go.item_id = ie.item_id
        where go.offer_product_id is null
        qualify
            row_number() over (
                partition by ie.item_id order by ie.reduction_method desc
            )
            = 1

    ),
    z as (
        select
            item_id,
            array(
                select cast(e as float64)
                from
                    unnest(
                        split(substr(name_embedding, 2, length(name_embedding) - 2))
                    ) e
            ) as embedding,
        from k
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
    candidates as (
        select
            case
                when o.item_id like 'link-%'
                then concat('offer-', o.offer_id)
                else o.item_id
            end as item_id,
            z.embedding,
            o.offer_name,
            o.offer_description,
            o.performer,
            o.offer_subcategory_id
        from offers o
        inner join z on z.item_id = o.item_id
        inner join
            `{{ bigquery_sandbox_dataset }}.unmatched_offers` uo
            on uo.item_id_candidate = o.item_id
    )
select *
from candidates
qualify row_number() over (partition by item_id order by performer desc) = 1
order by rand()
