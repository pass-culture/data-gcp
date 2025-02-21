WITH last_status_update AS (
    SELECT
        user_id,
        MAX(datecreated) AS last_modified_at
    FROM {{ source("raw", "applicative_database_beneficiary_fraud_check") }}
    WHERE
        type = 'PROFILE_COMPLETION'
        AND status = 'OK'
    GROUP BY user_id
),

user_location_update AS (
    SELECT
        user_id,
        MAX(date_updated) AS last_calculation_at
    FROM {{ source("raw", "user_locations") }}
    GROUP BY user_id
),

user_candidates AS (
    SELECT
        adu.user_id,
        adu.user_creation_date AS user_creation_at,
        GREATEST(adu.user_creation_date, lsu.last_modified_at) AS user_last_modified_at,
        TRIM(
            CONCAT(
                REPLACE(REPLACE(adu.user_address, '\\r', ''), '\\n', ''), ' ',
                adu.user_postal_code, ' ',
                adu.user_city
            )
        ) AS user_full_address
    FROM {{ source("raw", "applicative_database_user") }} AS adu
    LEFT JOIN last_status_update AS lsu
        ON adu.user_id = lsu.user_id
    WHERE
        COALESCE(adu.user_address, '') <> ''
        AND adu.user_postal_code IS NOT null
        AND adu.user_city IS NOT null
)


SELECT
    uc.user_id,
    uc.user_full_address,
    uc.user_creation_at,
    ulu.last_calculation_at AS user_address_last_calculation_at

FROM user_candidates AS uc
LEFT JOIN user_location_update AS ulu ON uc.user_id = ulu.user_id
WHERE
-- no location update or location update is older than last user update
    (
        ulu.user_id IS null
        OR ulu.last_calculation_at < uc.user_last_modified_at
    )
