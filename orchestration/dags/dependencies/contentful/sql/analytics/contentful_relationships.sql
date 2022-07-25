SELECT
    *
except
(row_number)
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY parent,
                child
                ORDER BY
                    execution_date DESC
            ) as row_number
        FROM
            `{{ bigquery_raw_dataset }}.contentful_relationships`
    ) inn
WHERE
    row_number = 1