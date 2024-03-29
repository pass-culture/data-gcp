SELECT
    CAST("id" AS varchar(255)) AS id
    , CAST("userId" AS varchar(255)) AS user_id
    , CAST("authorId" AS varchar(255)) AS author_id
    , review AS review
    , "dateReviewed" AT TIME ZONE \'UTC\' AT TIME ZONE \'Europe/Paris\' AS datereviewed
    , reason AS reason
    , CAST("eligibilityType" AS varchar(255)) AS eligibility_type
FROM public.beneficiary_fraud_review
