with
    import_embeddings as (
        select ie.item_id, ie.name_embedding
        from `{{ bigquery_ml_preproc_dataset }}.item_embedding_reduced_32` ie
        inner join
            `{{ bigquery_analytics_dataset }}.global_offer` go
            on go.item_id = ie.item_id
        where go.offer_product_id is not null
        qualify
            row_number() over (
                partition by ie.item_id order by ie.reduction_method desc
            )
            = 1

    ),
    prepocess_embeddings as (
        select
            item_id,
            array(
                select cast(e as float64)
                from
                    unnest(
                        split(substr(name_embedding, 2, length(name_embedding) - 2))
                    ) e
            ) as embedding,
        from import_embeddings
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
        where go.offer_product_id is not null
    ),
    sources as (
        select
            case
                when o.item_id like 'link-%'
                then concat('offer-', o.offer_id)
                else o.item_id
            end as item_id,
            prepocess_embeddings.embedding,
            o.offer_name,
            o.offer_description,
            o.performer,
            o.offer_subcategory_id
        from offers o
        inner join prepocess_embeddings on prepocess_embeddings.item_id = o.item_id
    )
select *
from sources
qualify row_number() over (partition by item_id order by performer desc) = 1
order by rand()
