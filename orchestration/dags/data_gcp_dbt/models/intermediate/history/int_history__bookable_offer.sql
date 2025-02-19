{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "partition_date", "data_type": "date"},
            require_partition_filter=true,
        )
    )
}}

select distinct
    date('{{ ds() }}') as partition_date,
    offer.offer_id,
    int_applicative__offer_item_id.item_id,
    global_offer.offer_subcategory_id,
    subcategories.category_id as offer_category_id
from {{ ref("snapshot__bookable_offer") }} as offer
left join
    {{ ref("int_applicative__offer") }} as global_offer
    on offer.offer_id = global_offer.offer_id
left join
    {{ ref("int_applicative__offer_item_id") }} as int_applicative__offer_item_id
    on offer.offer_id = int_applicative__offer_item_id.offer_id
left join
    {{ source("raw", "subcategories") }} as subcategories
    on global_offer.offer_subcategory_id = subcategories.id
where
    date('{{ ds() }}') >= date(offer.dbt_valid_from)
    and (offer.dbt_valid_to is null or date('{{ ds() }}') <= date(offer.dbt_valid_to))
