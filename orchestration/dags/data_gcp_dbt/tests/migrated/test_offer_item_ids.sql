
{% set legacy_query %}
  select
    *
  from analytics_{{ target.name }}.offer_item_ids
{% endset %}

{% set new_dbt_query %}
  select
    *
  from {{ ref('offer_item_ids') }}
{% endset %}

{{ audit_helper.compare_queries(
    a_query=legacy_query,
    b_query=new_dbt_query,
    primary_key="offer_id"
) }}