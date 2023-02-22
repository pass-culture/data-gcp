SELECT 
    CAST("invoiceId" AS varchar(255)) AS invoice_id
    , CAST("cashflowId" AS varchar(255)) cashflow_id
FROM public.invoice_cashflow