SELECT
    CAST("id" AS varchar(255)) AS collective_stock_id
    , CAST("stockId" AS varchar(255)) AS stock_id
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_stock_creation_date
    , "dateModified" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_stock_modification_date
    , "beginningDatetime" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_stock_beginning_date_time
    , CAST("collectiveOfferId" AS varchar(255)) AS collective_offer_id
    , "price" AS collective_stock_price
    , "bookingLimitDatetime" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS collective_stock_booking_limit_date_time
    , "numberOfTickets" AS collective_stock_number_of_tickets
    , "priceDetail" AS collective_stock_price_detail
FROM public.collective_stock
