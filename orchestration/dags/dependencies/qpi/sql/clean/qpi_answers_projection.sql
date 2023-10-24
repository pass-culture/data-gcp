with base as (
    SELECT
        * 
    FROM (select * from `{{ bigquery_raw_dataset }}.qpi_answers_v4`) as qpi, qpi.answers as answers
),
unnested_base as (
    SELECT 
        user_id
        , submitted_at
        , unnested AS answer_ids
    FROM base
    CROSS JOIN UNNEST(base.answer_ids) AS unnested
),
user_subcat as (
    select 
        b.user_id
        , submitted_at
        , map.subcategories 
    from unnested_base b 
    JOIN `{{ bigquery_clean_dataset }}.QPI_mapping` map
    ON b.answer_ids = map.answer_id
    WHERE b.answer_ids like 'PROJECTION_%'
    order by user_id
),
clean as (
    select 
        user_id
        , submitted_at
        , unnested as subcategories
    from user_subcat
    CROSS JOIN UNNEST(user_subcat.subcategories) AS unnested
), 
base_deduplicate as (
    select 
        user_id
        , submitted_at
        , subcategories
        , ROW_NUMBER() OVER (PARTITION BY user_id, subcategories order by subcategories DESC) row_number
    FROM clean 
    order by user_id
)
SELECT 
    * except(row_number) 
FROM base_deduplicate 
WHERE row_number = 1
AND subcategories is not null 
AND subcategories <> ""



