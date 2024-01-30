-- depends_on: {{ ref('enriched_offerer_tags_data') }}
{{ 
    compare_relations(
        'enriched_offerer_tags_data',
        'analytics',
        'offerer_tag_mapping_id'
    )
}}