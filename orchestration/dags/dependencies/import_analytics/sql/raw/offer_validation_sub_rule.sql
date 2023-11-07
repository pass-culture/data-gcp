SELECT 
    cast("id" as varchar(255)) as id,
    cast("validationRuleId" as varchar(255)) as validation_rule_id,
    cast("model" as varchar(255)) as model,
    cast("attribute" as varchar(255)) as attribute,
    cast("operator" as varchar(255)) as operator,
    cast("comparated" as json) as comparated
FROM public.offer_validation_sub_rule