with
    base_offers as (
        select
            eod.offer_id,
            eod.item_id,
            eod.offer_subcategory_id,
            evd.venue_type_label,
            evd.offerer_name
        from `{{ bigquery_analytics_dataset }}.global_offer` eod
        join `{{ bigquery_raw_dataset }}.applicative_database_offer` o using (offer_id)
        join
            `{{ bigquery_analytics_dataset }}.global_venue` evd
            on evd.venue_managing_offerer_id = eod.offerer_id
        where
            date(eod.offer_creation_date) >= date_sub(current_date, interval 6 month)
            and eod.offer_validation = "APPROVED"
            and eod.offer_product_id is null
            and eod.item_id not like 'isbn-%'
        group by 1, 2, 3, 4, 5
    )
select
    b.*,
    ie.name_embedding as name_embedding,
    ie.description_embedding as description_embedding
from base_offers b
join
    (
        select
            item_id,
            name_embedding as name_embedding,
            description_embedding as description_embedding
        from `{{ bigquery_ml_preproc_dataset }}.item_embedding_extraction`
        where date(extraction_date) >= date_sub(current_date, interval 6 month)
        qualify
            row_number() over (partition by item_id order by extraction_date desc) = 1
    ) ie using (item_id)
