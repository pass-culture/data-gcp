SELECT
    CAST("id" AS varchar(255)) as booking_id
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as booking_creation_date
    , CAST("stockId" AS varchar(255)) as stock_id
    , "quantity" as booking_quantity
    , CAST("userId" AS varchar(255)) as user_id
    , "amount" as booking_amount
    , CAST("status" AS varchar(255)) AS booking_status
    , "status" = \'CANCELLED\' AS booking_is_cancelled
    , "status" IN (\'USED\', \'REIMBURSED\') as booking_is_used
    , "dateUsed" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as booking_used_date
    , "cancellationDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as booking_cancellation_date
    , CAST("cancellationReason" AS VARCHAR) AS booking_cancellation_reason
    , CAST("individualBookingId" AS varchar(255)) as individual_booking_id
    , "reimbursementDate" AS booking_reimbursement_date
FROM public.booking
