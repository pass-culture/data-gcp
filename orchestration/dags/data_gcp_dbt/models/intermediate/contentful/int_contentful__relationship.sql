SELECT
    parent,
    child,
    execution_date
FROM {{ source('raw', 'contentful_relationship') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY parent,child ORDER BY execution_date DESC) = 1