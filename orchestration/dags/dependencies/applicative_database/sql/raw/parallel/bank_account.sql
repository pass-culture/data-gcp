SELECT
    "isActive" AS is_active
    ,CAST("id" AS VARCHAR(255)) AS bank_account_id
    ,CAST("label" AS VARCHAR(255)) AS bank_account_label
    ,CAST("offererId" AS VARCHAR(255)) AS offerer_id
    ,CAST("iban" AS VARCHAR(255)) AS iban
    ,CAST("bic" AS VARCHAR(255)) AS bic
    ,CAST("dsApplicationId" AS VARCHAR(255)) AS ds_application_id
    ,CAST("status" AS VARCHAR(255)) AS status
    ,"dateCreated" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\'  AS date_created
    ,"dateLastStatusUpdate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\'  AS date_last_status_update
FROM public.bank_account
