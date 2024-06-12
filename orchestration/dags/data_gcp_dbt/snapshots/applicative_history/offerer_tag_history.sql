{% snapshot offerer_tag_history %}
    
{{
    config(
      strategy='check',
      unique_key='venue_id',
      check_cols=['offerer_tag_id', 'offerer_tag_name', 'offerer_tag_label', 'offerer_tag_description']
    )
}}

SELECT
	offerer_tag_id,
	offerer_tag_name,
	offerer_tag_label,
	offerer_tag_description
FROM {{ ref('offerer_tag') }}

{% endsnapshot %}