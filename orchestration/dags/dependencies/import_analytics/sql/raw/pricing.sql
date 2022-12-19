SELECT
    CAST("id" AS varchar(255))
    , CAST("status" AS varchar(255))
    , CAST("bookingId" AS varchar(255))
    , CAST("collectiveBookingId" AS varchar(255)) AS collective_booking_id
    , CAST("businessUnitId" AS varchar(255))
    , "creationDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\'
    , "valueDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\'
    , "amount"
    , "standardRule"
    , CAST("customRuleId" AS varchar(255))
    , "revenue"
    , "siret"
FROM public.pricing
