SELECT
    *
FROM {{ source('raw', 'accessibility_provider') }}