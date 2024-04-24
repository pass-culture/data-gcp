SELECT
    CAST("id" AS varchar(255)) AS finance_event_id
    , "creationDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS finance_event_creation_date
    , "valueDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS finance_event_value_date
    , "pricingOrderingDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS finance_event_price_ordering_date
    , "status" AS finance_event_status
    , "motive" AS finance_event_motive
    , CAST("bookingId" AS varchar(255)) AS booking_id
    , CAST("collectiveBookingId" AS varchar(255)) AS collective_booking_id
    , CAST("venueId" AS varchar(255)) AS venue_id
    , CAST("pricingPointId" AS varchar(255)) AS pricing_point_id
FROM finance_event