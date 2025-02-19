{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="sync_all_columns",
        )
    )
}}

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
        when
            fe.user_id is not null
            and datetime(fe.event_timestamp)
            between eud.first_deposit_creation_date and eud.last_deposit_expiration_date
        then 'Bénéficiaire'
        when
            fe.user_id is not null
            and datetime(fe.event_timestamp) > eud.last_deposit_expiration_date
        then 'Ancien bénéficiaire'
        when fe.user_id is not null
        then 'Non bénéficiaire'
        else 'Non loggué'
    end as user_role,
    if(
        extract(dayofyear from fe.event_date)
        < extract(dayofyear from eud.user_birth_date),
        date_diff(fe.event_date, eud.user_birth_date, year) - 1,
        date_diff(fe.event_date, eud.user_birth_date, year)
    ) as user_age,
    count(*) as cnt_events
from {{ ref("int_firebase__native_event") }} fe
join
    {{ ref("mrt_global__offer") }}
    o
    on fe.offer_id = o.offer_id
    and fe.event_name
    in ('ConsultOffer', 'ConsultWholeOffer', 'ConsultDescriptionDetails')
left join
    {{ ref("int_contentful__algolia_modules_criterion") }}
    c on fe.module_id = c.module_id and fe.offer_id = c.offer_id
left join {{ ref("mrt_global__user") }} eud on fe.user_id = eud.user_id
where
    true
    {% if is_incremental() %}
        and event_date
        between date_sub(date("{{ ds() }}"), interval 3 day) and date("{{ ds() }}")
    {% else %}
        and date(event_date)
        >= date_sub('{{ ds() }}', interval {{ var("full_refresh_lookback") }})
    {% endif %}
group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14
