select
    fe.event_date,
    o.offerer_id,
    o.offerer_name,
    o.venue_id,
    o.venue_name,
    o.offer_id,
    o.offer_name,
    c.tag_name as name,
    fe.event_name,
    fe.traffic_medium,
    fe.traffic_campaign,
    fe.origin,
    case
        when fe.user_id is not NULL
            and DATETIME(fe.event_timestamp) between eud.user_deposit_creation_date
            and eud.user_deposit_expiration_date then 'Bénéficiaire'
        when fe.user_id is not NULL
            and DATETIME(fe.event_timestamp) > eud.user_deposit_expiration_date then 'Ancien bénéficiaire'
        when fe.user_id is not NULL then 'Non bénéficiaire'
        else 'Non loggué'
    end as user_role,
    IF(
        EXTRACT(
            dayofyear
            from
            fe.event_date
        ) < EXTRACT(
            dayofyear
            from
            eud.user_birth_date
        ),
        DATE_DIFF(fe.event_date, eud.user_birth_date, year) - 1,
        DATE_DIFF(fe.event_date, eud.user_birth_date, year)
    ) as user_age,
    COUNT(*) as cnt_events
from {{ ref('int_firebase__native_event') }} fe
    join
        {{ ref('mrt_global__offer') }}
            o
        on fe.offer_id = o.offer_id
            and fe.event_name in (
                'ConsultOffer',
                'ConsultWholeOffer',
                'ConsultDescriptionDetails'
            )
    left join
        {{ ref('int_contentful__algolia_modules_criterion') }}
            c
        on fe.module_id = c.module_id
            and fe.offer_id = c.offer_id
    left join {{ ref('enriched_user_data') }} eud on fe.user_id = eud.user_id
group by
    1,
    2,
    3,
    4,
    5,
    6,
    7,
    8,
    9,
    10,
    11,
    12,
    13,
    14
