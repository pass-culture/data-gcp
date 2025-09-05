with test_items as (
    select td.tag_name,go.item_id,td.offer_id,go.offer_name,go.offer_description
    from `passculture-data-prod.sandbox_prod.chatbot_test_dataset` td
    join `passculture-data-prod.analytics_prod.global_offer` go using (offer_id)
),
import_embeddings as (
    select ie.item_id, ie.semantic_content_embedding
    from `passculture-data-prod.ml_preproc_prod.item_embedding_extraction` ie
    inner join test_items ti on ti.item_id = ie.item_id
    qualify
        row_number() over (
            partition by ie.item_id order by ie.extraction_date desc
        )
        = 1
)
select
    * except(semantic_content_embedding),
    array(
        select cast(e as float64)
        from
            unnest(
                split(substr(semantic_content_embedding, 2, length(semantic_content_embedding) - 2))
            ) e
    ) as embedding
from import_embeddings
  