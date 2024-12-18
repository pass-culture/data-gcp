
SELECT
    CAST(ah."id" AS varchar(255)) AS action_history_id
    , ah."jsonData" AS action_history_json_data
    , ah."actionType" AS action_type
    , "actionDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS action_date
    , CAST(ah."authorUserId" AS VARCHAR(255)) AS author_user_id
    , CASE
        WHEN ah."actionType" = \'COMMENT\' THEN CAST(u."email" AS VARCHAR(255))
        WHEN ah."actionType" = \'USER_OFFERER_VALIDATED\' THEN CAST(u."email" AS VARCHAR(255))
        WHEN ah."actionType" = \'USER_OFFERER_PENDING\' THEN CAST(u."email" AS VARCHAR(255))
        WHEN ah."actionType" = \'USER_OFFERER_REJECTED\' THEN CAST(u."email" AS VARCHAR(255))
        WHEN ah."actionType" = \'OFFERER_VALIDATED\' THEN CAST(u."email" AS VARCHAR(255))
        WHEN ah."actionType" = \'OFFERER_PENDING\' THEN CAST(u."email" AS VARCHAR(255))
        WHEN ah."actionType" = \'OFFERER_REJECTED\' THEN CAST(u."email" AS VARCHAR(255))
        WHEN ah."actionType" = \'OFFERER_SUSPENDED\' THEN CAST(u."email" AS VARCHAR(255))
        WHEN ah."actionType" = \'OFFERER_UNSUSPENDED\' THEN CAST(u."email" AS VARCHAR(255))
    END AS author_email
    , CASE
        WHEN ah."actionType" = \'BLACKLIST_DOMAIN_NAME\' THEN CAST(ah."jsonData" -> \'domain\' AS VARCHAR(255))
    END AS blacklisted_domain
    , CAST(ah."userId" AS VARCHAR(255)) AS user_id
    , CAST(ah."offererId" AS VARCHAR(255)) AS offerer_id
    , CAST(ah."venueId" AS VARCHAR(255)) AS venue_id
    , ah."comment" AS comment
    ,REPLACE(CAST("jsonData" -> \'modified_info\' -> \'postalCode\' -> \'old_info\' AS VARCHAR(255)), \'"\', \'\') AS old_postal_code
    ,REPLACE(CAST("jsonData" -> \'modified_info\' -> \'postalCode\' -> \'new_info\' AS VARCHAR(255)), \'"\', \'\') AS new_postal_code
    ,REPLACE(CAST("jsonData" -> \'modified_info\' -> \'activity\' -> \'old_info\' AS VARCHAR(255)), \'"\', \'\') AS old_activity
    ,REPLACE(CAST("jsonData" -> \'modified_info\' -> \'activity\' -> \'new_info\' AS VARCHAR(255)), \'"\', \'\') AS new_activity
FROM public.action_history ah
LEFT JOIN public.user u on ah."authorUserId" = u.id
