{{ config(materialized="table", cluster_by=["item_id", "to_embed"]) }}

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
        where offer.item_id is not null
    ),

    deduplicated as (
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
        from deduplicated
    ),

    previous_state as (
        {% set self_relation = adapter.get_relation(
            database=target.database,
            schema=target.schema,
            identifier=this.identifier,
        ) %}
        {% if self_relation is not none %}select item_id, content_hash from {{ this }}
        {% else %}
            select cast(null as string) as item_id, cast(null as int64) as content_hash
            limit 0
        {% endif %}
    )

select
    wfp.*,
    case
        when ps.item_id is null
        then true  -- New item
        when ps.content_hash != wfp.content_hash
        then true  -- Changed content
        else false  -- Unchanged
    end as to_embed
from with_fingerprint as wfp
left join previous_state as ps on wfp.item_id = ps.item_id
