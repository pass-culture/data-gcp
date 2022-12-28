SELECT
    CAST("id" AS varchar(255)) AS offer_report_id
    , CAST("userId" AS varchar(255)) AS user_id
    , CAST("offerId" AS varchar(255)) AS offer_id
    , reason AS offer_report_reason
    , "customReasonContent" AS offer_report_custom_reason_content
    , "reportedAt" AS offer_report_date
FROM public.offer_report