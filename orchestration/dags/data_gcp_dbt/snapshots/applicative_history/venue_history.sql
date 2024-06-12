{% snapshot venue_history %}

{{
    config(
      strategy='check',
      unique_key='venue_id',
      check_cols=['venue_siret', 'venue_is_permanent', 'venue_type_code', 'venue_label_id', 'banner_url', 'venue_description', 'venue_audioDisabilityCompliant', 'venue_mentalDisabilityCompliant', 'venue_motorDisabilityCompliant', 'venue_visualDisabilityCompliant', 'venue_withdrawal_details']
    )
}}

select
  venue_id,
  venue_siret,
  venue_is_permanent,
  venue_type_code,
  venue_label_id,
  banner_url,
  venue_description,
  venue_audioDisabilityCompliant,
  venue_mentalDisabilityCompliant,
  venue_motorDisabilityCompliant,
  venue_visualDisabilityCompliant,
  venue_withdrawal_details
from {{ ref('venue') }}

{% endsnapshot %}


