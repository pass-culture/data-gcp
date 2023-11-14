SELECT 
    cast("id" as VARCHAR(255)) as id,
    cast("ruleId" as VARCHAR(255)) as rule_id,
    cast("collectiveOfferTemplateId" as VARCHAR(255)) as collective_offer_template_id
FROM public.validation_rule_collective_offer_template_link