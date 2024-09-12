SELECT bfd.id,
    bfd.datecreated,
    bfd.user_id,
    bfd.type,
    bfd.reason,
    bfd.reasoncodes,
    bfd.status,
    bfd.eligibility_type,
    bfd.thirdpartyid,
    bfd.result_content,
    u.user_is_active,
    ah.action_history_reason
FROM {{ source('raw', 'applicative_database_beneficiary_fraud_check') }} AS bfd
LEFT JOIN {{ ref('int_applicative__user') }} AS u ON u.user_id = bfd.user_id
LEFT JOIN {{ ref('int_applicative__action_history') }} AS ah ON ah.user_id = u.user_id
    AND action_history_rk = 1
