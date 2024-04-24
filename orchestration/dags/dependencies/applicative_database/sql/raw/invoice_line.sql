SELECT 
    CAST("id" AS varchar(255)) AS invoice_line_id
    ,CAST("invoiceId" AS varchar(255)) AS invoice_id
    ,"label"
    ,-"contributionAmount"/100 AS contribution_amount
    ,-"reimbursedAmount"/100 AS reimbursement_amount
    ,"rate"
    ,"group" ->> \'label\' AS bareme
FROM public.invoice_line


