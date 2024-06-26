SELECT
    id AS deposit_id,
    -- HOTFIX: Adjust 'amount' from 90 to 80 to correct a discrepancy (55 deposit are concerned)
    CASE WHEN type = "GRANT_15_17" AND amount > 80 THEN 80
        WHEN type = "GRANT_18" AND amount < 300 THEN 300
        WHEN type = "GRANT_18" AND amount > 500 THEN 500
        ELSE amount
    END AS deposit_amount,
    userId AS user_id,
    source,
    dateCreated AS deposit_creation_date,
    dateUpdated AS deposit_update_date,
    expirationDate AS deposit_expiration_date,
    type AS deposit_type,
    CASE WHEN lower(source) like "%educonnect%" THEN "EDUCONNECT"
        WHEN lower(source) like "%ubble%" THEN "UBBLE"
        WHEN (lower(source) like "%dms%" OR lower(source) like "%démarches simplifiées%") THEN "DMS"
        ELSE source END AS deposit_source,
    ROW_NUMBER() OVER(
            PARTITION BY userId
            ORDER BY
                dateCreated,
                id
        ) AS deposit_rank_asc,
    ROW_NUMBER() OVER(
            PARTITION BY userId
            ORDER BY
                dateCreated DESC,
                id DESC
        ) AS deposit_rank_desc
FROM {{ source("raw", "applicative_database_deposit") }} AS d

