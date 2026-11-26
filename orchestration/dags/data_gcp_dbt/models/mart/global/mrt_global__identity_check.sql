-- Identity checks only: the bonus credit checks (QF / AAH / AEEH) carry social and
-- disability data and are deleted by the backend once the bonus is granted.
-- Free text and third-party payloads (reason, thirdpartyid, result_content) are not
-- exposed.
select
    id as identity_check_id,
    user_id,
    type as identity_check_type,
    status as identity_check_status,
    eligibility_type as identity_check_eligibility_type,
    reasoncodes as identity_check_reason_code,
    datecreated as identity_check_creation_date,
    dateupdated as identity_check_update_date
from {{ ref("int_applicative__beneficiary_fraud_check") }}
where type in ('UBBLE', 'DMS', 'EDUCONNECT', 'JOUVE')
