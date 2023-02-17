SELECT
    CAST("id" AS varchar(255)) AS venue_contact_id
    , CAST("venueId" AS varchar(255)) AS venue_id
    , "email" AS venue_contact_email
    , "website" AS venue_contact_website
    , "phone_number" AS venue_contact_phone_number
    , "social_medias" AS venue_contact_social_medias
FROM public.venue_contact