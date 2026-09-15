with
    offers as (
        select
            go.item_id as raw_item_id,
            case
                when go.item_id like 'link-%'
                then concat('offer-', go.offer_id)
                else go.item_id
            end as item_id,
            go.offer_id,
            go.offer_name,
            go.offer_description,
            go.performer,
            go.offer_subcategory_id
        from `{{ bigquery_analytics_dataset }}.global_offer` go
        where go.offer_product_id is not null
        qualify
            row_number() over (
                partition by
                    case
                        when go.item_id like 'link-%'
                        then concat('offer-', go.offer_id)
                        else go.item_id
                    end
                order by go.performer desc
            )
            = 1
    ),
    sources as (
        select
            o.item_id,
            ie.semantic_content_128 as embedding,
            o.offer_name,
            o.offer_description,
            o.performer,
            o.offer_subcategory_id
        from offers o
        inner join
            `{{ bigquery_ml_feat_dataset }}.item_embedding_refactor_128` ie
            on ie.item_id = o.raw_item_id
    )
select *
from sources
