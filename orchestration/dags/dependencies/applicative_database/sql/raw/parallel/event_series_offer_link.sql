SELECT
    "id" as event_series_offer_link_id
    ,"eventSeriesId" as event_series_id
    ,"offerId" as offer_id
    ,"dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS date_created
    ,"dateModified" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS date_modified
FROM public.event_series_offer_link
