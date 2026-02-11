{{
    config(
        **custom_table_config(
            materialized="table",
            cluster_by=["item_id"],
        )
    )
}}

{% set config = get_semantic_embedding_feature_config() %}

{% set all_offer_cols = config.offer.embedding_features + config.offer.extra_data %}
{% set all_metadata_cols = (
    config.offer_metadata.embedding_features
    + config.offer_metadata.extra_data
) %}


{% set offer_feat_cols = config.offer.embedding_features %}
{% set metadata_feat_cols = config.offer_metadata.embedding_features %}
{% set fingerprinted_features = (
    (offer_feat_cols + metadata_feat_cols) | unique | sort
) %}  -- important to sort for stable fingerprinting


with
    enriched_items as (
        select
            offer.offer_id,
            offer.item_id,
            {% for col in all_offer_cols -%} offer.{{ col }}, {% endfor -%}
            {% for col in all_metadata_cols -%} meta.{{ col }}, {% endfor -%}
            -- features for embedding freshness and deduplication
            if(
                meta.offer_type_label is not null,
                offer.total_used_individual_bookings,
                null
            ) as total_used_individual_bookings
        from {{ ref("mrt_global__offer") }} as offer
        left join
            {{ ref("mrt_global__offer_metadata") }} as meta
            on offer.offer_id = meta.offer_id
    ),

    deduplicated_items as (
        select * except (total_used_individual_bookings, offer_id)
        from enriched_items
        qualify
            row_number() over (
                partition by item_id
                order by total_used_individual_bookings desc, offer_creation_date asc
            )
            = 1
    )

select
    item_id,
    {% for col in (all_offer_cols + all_metadata_cols) -%} {{ col }}, {% endfor -%}
    -- build a content hash based on the features used to detect
    -- metadata changes and trigger re-embedding accordingly
    -- handles the "Invalid cast" by serializing the whole struct to JSON first
    format(
        '%016x',
        farm_fingerprint(
            to_json_string(
                struct(
                    {% for feat in fingerprinted_features -%}
                        {{ feat }}{{ "," if not loop.last else "" }}
                    {% endfor -%}
                )
            )
        )
    ) as content_hash
from deduplicated_items
