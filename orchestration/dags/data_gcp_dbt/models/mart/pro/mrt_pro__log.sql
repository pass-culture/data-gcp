SELECT
   environement,
   user_id,
   message,
   booking_id,
   offer_id,
   venue_id,
   product_id,
   stock_id,
   stock_old_quantity,
   stock_new_quantity,
   stock_old_price,
   stock_new_price,
   stock_booking_quantity,
   list_of_eans_not_found,
   log_timestamp,
FROM {{ref('int_pcapi__log')}}
WHERE partition_date >= DATE_SUB(CURRENT_DATE, INTERVAL 90 day)
    AND message IN ("Booking has been cancelled"
        ,"Offer has been created"
        ,"Offer has been updated"
        ,"Booking was marked as used"
        ,"Booking was marked as unused"
        ,"Successfully updated stock"
        ,"Some provided eans were not found"
        ,"Stock update blocked because of price limitation"
    )
