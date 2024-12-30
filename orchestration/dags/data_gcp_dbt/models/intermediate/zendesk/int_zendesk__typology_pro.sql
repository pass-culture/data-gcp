WITH formatted_typologies AS (
  SELECT
    zendesk_ticket_id,
    zendesk_ticket_created_at,
    zendesk_ticket_created_date,
    -- Generate zendesk_typology_id as a hash of the base string
    "support_pro" AS zendesk_typology_type,
    TO_HEX(SHA256(CONCAT(spt_unnest))) AS zendesk_typology_id,
    -- Split the typology into primary and secondary parts
    SPLIT(spt_unnest, "__")[SAFE_OFFSET(0)] AS zendesk_typology_primary,
    SPLIT(spt_unnest, "__")[SAFE_OFFSET(1)] AS zendesk_typology_secondary

  FROM {{ ref("int_zendesk__ticket") }},
    UNNEST(zendesk_typology_support_pro) AS spt_unnest
)

SELECT
  zendesk_ticket_created_at,
  zendesk_ticket_created_date,
  zendesk_ticket_id,
  zendesk_typology_type,
  zendesk_typology_id,
  zendesk_typology_primary,
  zendesk_typology_secondary
FROM formatted_typologies
