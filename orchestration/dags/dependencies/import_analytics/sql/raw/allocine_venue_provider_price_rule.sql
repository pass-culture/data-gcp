SELECT
    CAST("id" AS varchar(255))
    , CAST("allocineVenueProviderId" AS varchar(255))
    , CAST("priceRule" AS varchar(255))
    , "price"
FROM public.allocine_venue_provider_price_rule