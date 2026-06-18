SELECT
    "id" as event_series_id
    ,"name" as event_series_name
    ,"description" as event_series_description
    ,"mediationUuid" as event_series_mediation_uuid
    ,"dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS date_created
    ,"dateModified" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS date_modified
FROM public.event_series
