{% snapshot offerer_tag_history %}
    
{{
    config(
      strategy='check',
      unique_key='offerer_tag_id',
      check_cols=['offerer_tag_name', 'offerer_tag_label', 'offerer_tag_description']
    )
}}

SELECT
	offerer_tag_id,
	offerer_tag_name,
	offerer_tag_label,
	offerer_tag_description
FROM {{ source('raw', 'applicative_database_offerer_tag') }}

{% endsnapshot %}