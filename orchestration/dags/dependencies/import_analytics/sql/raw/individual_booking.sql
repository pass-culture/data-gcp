SELECT
    CAST("id" AS varchar(255)) AS individual_booking_id
    , CAST("userId" AS varchar(255)) AS user_id
    , CAST("depositId" AS varchar(255)) AS deposit_id
FROM individual_booking  