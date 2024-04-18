SELECT
    *
FROM {{ source('raw', 'contentful_relationships') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY parent,child ORDER BY execution_date DESC) = 1