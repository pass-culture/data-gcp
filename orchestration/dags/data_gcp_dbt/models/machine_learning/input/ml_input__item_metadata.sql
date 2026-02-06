{{ config(materialized="table", cluster_by=["to_embed", "item_id"]) }}

with
    enriched_items as (
        select
            offer.offer_id,
            offer.item_id,
            offer.offer_creation_date,
            offer.offer_subcategory_id,
            offer.offer_category_id,
            offer.offer_name,
            offer.offer_description,
            offer.offer_type_domain,
            offer.author,
            offer.performer,
            offer.titelive_gtl_id,
            offer_metadata.search_group_name,
            offer_metadata.image_url,
            offer_metadata.offer_type_id,
            offer_metadata.offer_sub_type_id,
            offer_metadata.gtl_type,
            offer_metadata.gtl_label_level_1,
            offer_metadata.gtl_label_level_2,
            offer_metadata.gtl_label_level_3,
            offer_metadata.gtl_label_level_4,
            offer_metadata.offer_type_label,
            offer_metadata.offer_type_labels,
            offer_metadata.offer_sub_type_label,
            if(
                offer_metadata.offer_type_label is not null,
                offer.total_used_individual_bookings,
                null
            ) as total_used_individual_bookings
        from {{ ref("mrt_global__offer") }} as offer
        left join
            {{ ref("mrt_global__offer_metadata") }} as offer_metadata
            on offer.offer_id = offer_metadata.offer_id
        left join
            {{ source("ml_preproc", "item_embedding_extraction") }} as ie
            on offer.item_id = ie.item_id
        where offer.item_id is not null
    ),

    deduplicated_items as (
        select * except (total_used_individual_bookings, offer_id)
        from enriched_items
        qualify
            row_number() over (
                partition by item_id order by total_used_individual_bookings desc
            )
            = 1
    ),

    with_fingerprint as (
        select
            *,
            farm_fingerprint(
                to_json_string(
                    struct(
                        offer_name,
                        offer_description,
                        image_url,
                        titelive_gtl_id,
                        gtl_label_level_1,
                        gtl_label_level_2,
                        gtl_label_level_3,
                        gtl_label_level_4,
                        offer_type_labels,
                        author,
                        performer
                    )
                )
            ) as content_hash
        from deduplicated_items
    ),

    previous_state as (
        select item_id, content_hash as content_hash_at_last_embedding
        from {{ source("ml_preproc", "item_embedding_extraction") }}
    )

select
    wfp.offer_id,
    wfp.item_id,
    wfp.offer_creation_date,
    wfp.offer_subcategory_id,
    wfp.offer_category_id,
    wfp.offer_name,
    wfp.offer_description,
    wfp.offer_type_domain,
    wfp.author,
    wfp.performer,
    wfp.titelive_gtl_id,
    wfp.search_group_name,
    wfp.image_url,
    wfp.offer_type_id,
    wfp.offer_sub_type_id,
    wfp.gtl_type,
    wfp.gtl_label_level_1,
    wfp.gtl_label_level_2,
    wfp.gtl_label_level_3,
    wfp.gtl_label_level_4,
    wfp.offer_type_label,
    wfp.offer_type_labels,
    wfp.offer_sub_type_label,
    wfp.total_used_individual_bookings,
    wfp.content_hash,
    case
        when ps.item_id is null
        then true  -- New item
        when wfp.content_hash != ps.content_hash_at_last_embedding
        then true  -- Changed content since last embedding
        else false  -- Unchanged
    end as to_embed
from with_fingerprint as wfp
left join previous_state as ps on wfp.item_id = ps.item_id
