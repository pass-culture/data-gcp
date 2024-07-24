{% snapshot snapshot__applicative_venue_criterion %}

{{
    config(
      strategy='check',
      unique_key='venue_criterion_id',
      check_cols=['venue_id', 'criterion_id']
    )
}}

SELECT
	venue_criterion_id,
  venue_id,
	criterion_id
FROM {{source('raw', 'applicative_database_venue_criterion') }}

{% endsnapshot %}
