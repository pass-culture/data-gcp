SELECT
    CAST("id" AS varchar(255)) as id
    , CAST("name" AS varchar(255)) as highlight_name
    , CAST("description" as varchar(255)) as highlight_description
    , CAST("mediation_uuid" as varchar(255)) as mediation_uuid
    , CAST("highlight_datespan" as varchar(255)) as highlight_datespan
    , CAST("availability_datespan" as varchar(255)) as availability_datespan
    , CAST("communication_date" as varchar(255))  AS communication_date
FROM public.highlight
