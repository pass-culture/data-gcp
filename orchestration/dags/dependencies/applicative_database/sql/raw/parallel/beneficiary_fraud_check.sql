SELECT
    id
    , datecreated
    , user_id
    , type
    , regexp_replace(reason, \'\\(.*?\\)\', \'(XXX)\') as reason
    , reasonCodes
    , status
    , eligibility_type
    , thirdpartyid
    , regexp_replace(content, \'"(email|phone|lastName|firstName|phoneNumber|reason_code|account_email|last_name|first_name|phone_number|id_piece_number)": "[^"]*",\' ,\'"\\1":"XXX",\', \'g\') as result_content
FROM (
    SELECT
        CAST("id" AS varchar(255)) AS id
        , "dateCreated" AS datecreated
        , CAST("userId" AS varchar(255)) AS user_id
        , type AS type
        , "reason" AS reason
        , "reasonCodes"[1] AS reasonCodes
        , "status" AS status
        , "eligibilityType" AS eligibility_type
        , "thirdPartyId" AS thirdpartyid
        , CAST("resultContent" AS text) as content
    FROM public.beneficiary_fraud_check
    ) AS data
