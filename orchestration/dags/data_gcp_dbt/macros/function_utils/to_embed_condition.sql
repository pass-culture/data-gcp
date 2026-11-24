{% macro to_embed_condition(current_alias, past_alias) %}
    {#- True when the item was never embedded for this vector or its content_hash
    changed since. past_alias must be the vector's own ml_semantic_embedding table. -#}
    (
        {{ past_alias }}.item_id is null
        or {{ current_alias }}.content_hash
        != coalesce({{ past_alias }}.content_hash, '')
    )
{% endmacro %}
