{% snapshot offerer_tag_mapping_history %}
    
{{
    config(
      strategy='check',
      unique_key='venue_id',
      check_cols=['offerer_tag_mapping_id', 'offerer_id', 'tag_id']
    )
}}

SELECT
	offerer_tag_mapping_id,
	offerer_id,
	tag_id
FROM {{ source('raw', 'applicative_database_offerer_tag_mapping') }}


{% endsnapshot %}