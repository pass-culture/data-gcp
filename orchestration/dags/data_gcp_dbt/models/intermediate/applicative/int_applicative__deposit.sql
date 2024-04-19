SELECT
    id,
    -- HOTFIX: Adjust 'amount' from 90 to 80 to correct a discrepancy (55 deposit are concerned)
    CASE WHEN type = "GRANT_15_17" AND amount > 80 THEN 80
        WHEN type = "GRANT_18" AND amount < 300 THEN 300
        WHEN type = "GRANT_18" AND amount > 500 THEN 500
        ELSE amount
    END AS amount,
    userId,
    source,
    dateCreated,
    dateUpdated,
    expirationDate,
    type
FROM {{ source("raw", "applicative_database_deposit") }}
