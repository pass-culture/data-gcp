SELECT
    CAST("id" AS varchar(255)) as address_id,
    CAST("banId" AS varchar(255)) as address_ban_id,
    CAST("inseeCode" AS varchar(255)) as address_insee_code,
    CAST("street" AS varchar(255)) as address_street,
    "postalCode" as address_postal_code,
    "city" as address_city,
    "latitude" as address_latitude,
    "longitude" as address_longitude,
    "departmentCode" as address_departement_code
FROM public.address