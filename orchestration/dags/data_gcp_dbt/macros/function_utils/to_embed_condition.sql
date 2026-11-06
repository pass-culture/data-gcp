-- Whether an item needs (re-)embedding for one vector: true if it was never
-- embedded for that vector (past.item_id is null) or its content changed since
-- the last successful embed *for that vector specifically*. Each vector must
-- compare against its own ml_semantic_embedding__<vector> merge table, not a
-- shared one, so a transient failure on one vector's embed/load can't have
-- another vector's unrelated success wrongly clear its retry flag.
{% macro to_embed_condition(current_alias, past_alias) %}
    (
        {{ past_alias }}.item_id is null
        or (
            {{ current_alias }}.content_hash
            != coalesce({{ past_alias }}.content_hash, '')
        )
    )
{% endmacro %}
