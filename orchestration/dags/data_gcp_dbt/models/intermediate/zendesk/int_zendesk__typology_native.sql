with
    formatted_typologies as (
        select
            zt.zendesk_ticket_id,
            zt.zendesk_ticket_created_at,
            zt.zendesk_ticket_created_date,
            -- Generate zendesk_typology_id as a hash of the base string
            "support_native" as zendesk_typology_type,
            -- Split the typology into primary and secondary parts
            to_hex(sha256(concat(spt_unnest))) as zendesk_typology_id,
            split(spt_unnest, "__")[safe_offset(0)] as zendesk_typology_primary,
            split(spt_unnest, "__")[safe_offset(1)] as zendesk_typology_secondary

        from
            {{ ref("int_zendesk__ticket") }} as zt,
            unnest(zt.zendesk_typology_support_native) as spt_unnest
    )

select
    zendesk_ticket_created_at,
    zendesk_ticket_created_date,
    zendesk_ticket_id,
    zendesk_typology_type,
    zendesk_typology_id,
    zendesk_typology_primary,
    zendesk_typology_secondary
from formatted_typologies
