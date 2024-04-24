SELECT 
    cast("id" as VARCHAR(255)) as id,
    cast("ruleId" as VARCHAR(255)) as rule_id,
    cast("collectiveOfferId" as VARCHAR(255)) as collective_offer_id
FROM public.validation_rule_collective_offer_link