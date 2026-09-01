SELECT
    CAST("id" AS varchar(255)) AS collective_additional_fee_id
    , CAST("collectiveStockId" AS varchar(255)) AS collective_stock_id
    , "type" AS collective_additional_fee_type
    , "label" AS collective_additional_fee_label
    , "amount" AS collective_additional_fee_amount
FROM public.collective_additional_fee
