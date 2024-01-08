SELECT 
    cod.collective_offer_id,
    cod.educational_domain_id,
    ed.educational_domain_name
FROM `{{ bigquery_clean_dataset }}`.`collective_offer_domain` cod
LEFT JOIN `{{ bigquery_clean_dataset }}`.`educational_domain` ed ON cod.educational_domain_id = ed.educational_domain_id