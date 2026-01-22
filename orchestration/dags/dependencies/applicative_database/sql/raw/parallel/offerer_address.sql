SELECT
    CAST("id" AS varchar(255)) as offerer_address_id,
    "label"  as offerer_address_label,
    CAST("addressId" AS varchar(255)) as address_id,
    CAST("offererId" AS varchar(255)) as offerer_id,
    CAST("venueId" AS varchar(255)) as venue_id
FROM public.offerer_address
