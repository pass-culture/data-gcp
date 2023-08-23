SELECT
    CAST("id" AS varchar(255)) AS national_program_offer_link_history_id
    , "dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS national_program_offer_link_history_creation_date
    , CAST("collectiveOfferId" AS varchar(255)) AS collective_offer_id
    , CAST("nationalProgramId" AS varchar(255)) AS national_program_id
 FROM public.national_program_offer_link_history