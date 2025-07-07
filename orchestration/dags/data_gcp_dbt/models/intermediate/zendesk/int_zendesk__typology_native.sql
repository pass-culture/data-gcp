with
    formatted_typologies as (
        select
            zt.ticket_id,
            zt.ticket_created_at,
            zt.ticket_created_date,
            zt.ticket_status,
            -- Generate zendesk_typology_id as a hash of the base string
            "support_native" as zendesk_typology_type,
            -- Split the typology into primary and secondary parts
            to_hex(sha256(concat(spt_unnest))) as zendesk_typology_id,
            split(spt_unnest, "__")[safe_offset(0)] as zendesk_typology_primary,
            split(spt_unnest, "__")[safe_offset(1)] as zendesk_typology_secondary

        from
            {{ ref("int_zendesk__ticket") }} as zt,
            unnest(zt.zendesk_typology_support_native) as spt_unnest
        where zt.ticket_status = "closed"
    )

select
    ticket_created_at,
    ticket_created_date,
    ticket_id,
    ticket_status,
    zendesk_typology_type,
    zendesk_typology_id,
    zendesk_typology_primary,
    zendesk_typology_secondary
from formatted_typologies
