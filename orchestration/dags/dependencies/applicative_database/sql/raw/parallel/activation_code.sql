SELECT
    CAST("id" AS varchar(255)) AS activation_code_id
    , CAST("stockId" AS varchar(255)) AS stock_id
    , CAST("bookingId" AS varchar(255)) AS booking_id
    , "expirationDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS activation_code_expiration_date
FROM public.activation_code
