SELECT
    CAST("id" AS varchar(255)) AS action_history_id
    , "jsonData" AS action_history_json_data
    , "actionType" AS action_type
    , "actionDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS action_date
    , CAST("authorUserId" AS VARCHAR(255)) AS author_user_id
    , CAST("userId" AS VARCHAR(255)) AS user_id
    , CAST("offererId" AS VARCHAR(255)) AS offerer_id
    , CAST("venueId" AS VARCHAR(255)) AS venue_id
    , "comment" AS comment
FROM action_history