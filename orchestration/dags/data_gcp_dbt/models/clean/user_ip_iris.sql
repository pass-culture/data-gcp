with all_ips as (
    select distinct
        DATE_TRUNC(DATE(timestamp), month) as month_log,
        jsonpayload.extra.sourceip as ip,
        jsonpayload.user_id as user_id
    from {{ source('raw', 'stdout') }}
    where DATE_TRUNC(DATE(timestamp), month) = DATE_TRUNC(DATE('{{ ds() }}'), month)
        and jsonpayload.extra.route in ('/native/v1/me', '/native/v1/signin')
        and jsonpayload.user_id is not null
),

ipv4 as (
    select distinct
        ip,
        NET.SAFE_IP_FROM_STRING(ip) as ip_bytes
    from all_ips
    where BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(ip)) = 4
),

ipv4d as (
    select
        ip,
        city_name,
        country_name,
        latitude,
        longitude
    from (
        select
            ip,
            ip_bytes & NET.IP_NET_MASK(4, mask) network_bin,
            mask
        from ipv4, UNNEST(GENERATE_ARRAY(8, 32)) mask
    )
        join {{ source('raw', 'geoip_city_v4') }}
            using (network_bin, mask)
    where country_name = "France"

),

ip_aggreg as (
    select
        month_log,
        CAST(user_id as string) as user_id,
        longitude,
        latitude,
        COUNT(*) nb_log_ip
    from all_ips ai
        left join ipv4d pv on pv.ip = ai.ip
    group by 1, 2, 3, 4
),

ip_ranking as (
    select
        *,
        ROW_NUMBER() over (partition by month_log, user_id order by nb_log_ip desc) ranking
    from ip_aggreg
)


select
    ip_ranking.month_log,
    ip_ranking.user_id,
    ip_ranking.nb_log_ip,
    iris_france.iriscode as ip_iris,
    iris_france.department
from ip_ranking
    cross join {{ source('clean', 'iris_france') }} as iris_france
where ranking = 1
    and ST_CONTAINS(iris_france.shape, ST_GEOGPOINT(longitude, latitude))
