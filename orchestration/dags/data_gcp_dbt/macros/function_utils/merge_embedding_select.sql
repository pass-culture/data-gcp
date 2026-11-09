-- The variable part of a per-vector embedding merge. The item_embedding DAG
-- appends every run to ml_preproc.item_embedding__<vector> (day-partitioned on
-- embedding_date), so this reads the history, not just the latest run.
--
-- * Incremental runs only re-read the last 3 days before the newest
-- embedding_date already merged (longer than the longest DAG run, so a run
-- loaded after a later one is still picked up; re-merging is idempotent).
-- * Several embeddings of the same item are reduced to the most recent one
-- (embedding_ts; legacy rows without it sort last).
-- * Until the first DAG run created the table, merge nothing instead of failing.
{% macro merge_embedding_select(source_relation, embedding_column) %}
    {% set table_exists = (
        not execute
        or adapter.get_relation(
            database=source_relation.database,
            schema=source_relation.schema,
            identifier=source_relation.identifier,
        )
        is not none
    ) %}
    {% if table_exists %}
        select * except (rn)
        from
            (
                select
                    src.item_id,
                    src.content_hash,
                    src.embedding as {{ embedding_column }},
                    src.mlflow_run_id,
                    src.embedding_model,
                    src.embedding_date,
                    src.embedding_ts,
                    row_number() over (
                        partition by src.item_id
                        order by
                            src.embedding_ts desc nulls last, src.embedding_date desc
                    ) as rn
                from {{ source_relation }} as src
                {% if is_incremental() %}
                    where
                        src.embedding_date >= date_sub(
                            (
                                select coalesce(max(embedding_date), date '1970-01-01')
                                from {{ this }}
                            ),
                            interval 3 day
                        )
                {% endif %}
            )
        where rn = 1
    {% else %}
        select *
        from
            unnest(
                array<
                    struct<
                        item_id string,
                        content_hash string,
                        {{ embedding_column }} array<float64>,
                        mlflow_run_id string,
                        embedding_model string,
                        embedding_date date,
                        embedding_ts timestamp
                    >
                >[]
            )
    {% endif %}
{% endmacro %}
