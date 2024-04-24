SELECT 
    *
FROM {{ source('raw', 'opening_hours') }}