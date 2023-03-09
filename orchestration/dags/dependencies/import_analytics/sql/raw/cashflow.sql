SELECT
    CAST("id" AS varchar(255))
    , "creationDate" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' as creationDate
    , "status"
    , CAST("bankAccountId" AS varchar(255))
    , CAST("batchId" AS varchar(255))
    , "amount"
    , CAST("reimbursementPointId" AS varchar(255)) AS reimbursement_point_id
FROM public.cashflow