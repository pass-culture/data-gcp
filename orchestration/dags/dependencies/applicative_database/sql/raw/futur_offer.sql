SELECT
    CAST("id" AS varchar(255)) as futur_offer_id,
    CAST("offerId" AS varchar(255)) as offer_id,
    "publicationDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as offer_publication_date
FROM public.futurOffer