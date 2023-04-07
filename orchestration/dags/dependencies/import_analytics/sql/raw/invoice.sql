SELECT 
    CAST("id" AS varchar(255)) AS invoice_id
    , "date" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS invoice_creation_date
    , "reference" AS invoice_reference
    , -"amount"/100 AS amount
    , CAST("reimbursementPointId" AS varchar(255)) AS reimbursement_point_id
FROM public.invoice