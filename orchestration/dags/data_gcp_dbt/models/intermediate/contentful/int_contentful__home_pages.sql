WITH home AS (
    SELECT
        date_imported,
        id AS home_id,
        title AS home_name,
        REPLACE(modules, '\"', "") AS module_id
    FROM {{ source('raw', 'contentful_entries') }},
        UNNEST(JSON_EXTRACT_ARRAY(modules, '$')) AS modules
    WHERE
        content_type = "homepageNatif"
),
home_and_modules AS (
    SELECT
        DATE(home.date_imported) AS date,
        home_id,
        home_name,
        title AS module_name,
        module_id,
        content_type
    FROM home
    LEFT JOIN {{ source('raw', 'contentful_entries') }} module 
    ON home.module_id = module.id
    AND home.date_imported = module.date_imported
),
contentful_entries AS (
    SELECT
        id,
        title,
        content_type,
        ROW_NUMBER() OVER (
            PARTITION BY id
            ORDER BY
                execution_date DESC
        ) AS rnk
    FROM{{ ref("int_contentful__entries") }}
),
SELECT
    date,
    home_id,
    home_name,
    module_name,
    module_id,
    content_type
FROM
    home_and_modules
QUALIFY ROW_NUMBER() OVER (PARTITION BY date, home_id, module_id) = 1