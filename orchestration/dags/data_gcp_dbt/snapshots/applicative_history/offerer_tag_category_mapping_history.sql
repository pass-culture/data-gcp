{% snapshot offerer_tag_category_mapping_history %}
    
{{
    config(
      strategy='check',
      unique_key='venue_id',
      check_cols=['offerer_tag_category_mapping_id', 'offerer_tag_id', 'offerer_tag_category_id']
    )
}}

SELECT
	offerer_tag_category_mapping_id,
	offerer_tag_id,
	offerer_tag_category_id
FROM {{ source('raw', 'applicative_database_offerer_tag_category_mapping') }}

{% endsnapshot %}