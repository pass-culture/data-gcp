SELECT
    *
    , ROW_NUMBER() OVER (
        PARTITION BY userId
        ORDER BY
            CAST(id AS INTEGER) DESC
    ) AS rank
FROM `{{ bigquery_raw_dataset }}`.applicative_database_user_suspension