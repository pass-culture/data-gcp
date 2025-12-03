select
    hr.id as highlight_request_id,
    hr.offer_id,
    hr.highlight_id,
    h.mediation_uuid as highlight_image_id,
    h.highlight_name,
    h.highlight_description,
    h.communication_date as highlight_communication_date,
    safe.parse_date(
        '%Y-%m-%d', regexp_extract(h.highlight_datespan, r'\[([^,]+)')
    ) as highlight_beginning_date,
    safe.parse_date(
        '%Y-%m-%d', regexp_extract(h.highlight_datespan, r',([^)]+)')
    ) as highlight_ending_date,
    safe.parse_date(
        '%Y-%m-%d', regexp_extract(h.availability_datespan, r'\[([^,]+)')
    ) as highlight_availability_beginning_date,
    safe.parse_date(
        '%Y-%m-%d', regexp_extract(h.availability_datespan, r',([^)]+)')
    ) as highlight_availability_ending_date
from {{ source("raw", "applicative_database_highlight_request") }} as hr
left join
    {{ source("raw", "applicative_database_highlight") }} as h on hr.highlight_id = h.id
