import pandas as pd


def enrich_answers(gcp_project, bigquery_clean_dataset, qpi_table):

    return f"""
        with base as (
            SELECT * FROM (select * from `{gcp_project}.{bigquery_clean_dataset}.temp_{qpi_table}`) as qpi, qpi.answers as answers
        ),
        unnested_base as (
        SELECT user_id,unnested AS answer_ids
        FROM base
        CROSS JOIN UNNEST(base.answer_ids) AS unnested
        ),
        user_subcat as (
        select b.user_id,map.subcategories from unnested_base b 
        JOIN `{gcp_project}.{bigquery_clean_dataset}.QPI_mapping` map 
        ON b.answer_ids = map.answer_id
        order by user_id
        ), clean as(
        select user_id, unnested as subcategories
        from user_subcat
        CROSS JOIN UNNEST(user_subcat.subcategories) AS unnested
        )
        select user_id, subcategories FROM clean 
        order by user_id
    """
