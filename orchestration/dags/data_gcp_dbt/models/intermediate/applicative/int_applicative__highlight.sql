select
    hr.id,
    hr.offer_id,
    hr.highlight_id,
    hr.mediation_uuid,
    h.highlight_name,
    h.highlight_description,
    safe.parse_date(
        '%Y-%m-%d', regexp_extract(hr.highlight_datespan, r'\[([^,]+)')
    ) as highlight_beginning_date,
    safe.parse_date(
        '%Y-%m-%d', regexp_extract(hr.highlight_datespan, r',([^)]+)')
    ) as highlight_ending_date,
    safe.parse_date(
        '%Y-%m-%d', regexp_extract(hr.availability_datespan, r'\[([^,]+)')
    ) as highlight_availability_beginning_date,
    safe.parse_date(
        '%Y-%m-%d', regexp_extract(hr.availability_datespan, r',([^)]+)')
    ) as highlight_availability_ending_date,
    hr.communication_date as highlight_communication_date
from {{ source("raw", "applicative_database_highlight_request") }} hr
left join {{ source("raw", "applicative_database_highlight") }} h on hr.id = h.id
