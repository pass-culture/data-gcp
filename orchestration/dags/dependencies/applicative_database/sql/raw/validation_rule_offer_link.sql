SELECT 
    cast("id" as VARCHAR(255)) as id,
    cast("ruleId" as VARCHAR(255)) as rule_id,
    cast("offerId" as VARCHAR(255)) as offer_id
FROM public.validation_rule_offer_link