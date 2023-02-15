WITH home AS (
    SELECT
        date_imported,
        id AS home_id,
        title AS home_name,
        REPLACE(modules, '\"', "") AS module_id
    FROM
        `{{ bigquery_raw_dataset }}.contentful_entries`,
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
    FROM
        home
        LEFT JOIN `{{ bigquery_raw_dataset }}.contentful_entries` module ON home.module_id = module.id
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
    FROM
        `{{ bigquery_raw_dataset }}.contentful_entries`
),
-- Legacy purposes
firebase_modules AS (
    SELECT
        event_date AS date,
        entry_id AS home_id,
        module_id AS module_id,
        MAX(homes.title) AS home_name,
        MAX(coalesce(modules.title, module_name)) AS module_name,
        MAX(modules.content_type) AS content_type
    FROM
        `{{ bigquery_analytics_dataset }}.firebase_events` firebase_events
        LEFT JOIN contentful_entries homes ON homes.id = firebase_events.entry_id
        AND homes.rnk = 1
        LEFT JOIN contentful_entries modules ON modules.id = firebase_events.module_id
    WHERE
        event_name = "ModuleDisplayedOnHomePage"
    GROUP BY
        event_date,
        module_id,
        home_id
),
all_modules AS (
    SELECT
        1 AS priority,
        'contentful' as origin,
        date,
        home_id,
        home_name,
        module_name,
        module_id,
        content_type
    FROM
        home_and_modules
    UNION
    ALL
    SELECT
        2 AS priority,
        'firebase' as origin,
        date,
        home_id,
        home_name,
        module_name,
        module_id,
        content_type
    FROM
        firebase_modules
)
SELECT
    *
except
(rnk, priority)
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY date,
                home_id,
                module_id
                ORDER BY
                    priority
            ) rnk
        FROM
            all_modules
    ) inn
WHERE
    rnk = 1
