SELECT
    CAST("id" AS varchar(255)) as id
    , CAST("status" AS varchar(255)) as status
    , CAST("bookingId" AS varchar(255)) as bookingId
    , CAST("collectiveBookingId" AS varchar(255)) AS collective_booking_id
    , CAST("businessUnitId" AS varchar(255)) as businessUnitId
    , "creationDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as creationDate
    , "valueDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as valueDate
    , "amount"
    , "standardRule"
    , CAST("customRuleId" AS varchar(255)) as customRuleId
    , "revenue"
    , "siret"
FROM public.pricing
