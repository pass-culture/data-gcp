
WITH all_ips AS (
  SELECT 
    distinct 
    DATE_TRUNC(DATE(timestamp), MONTH) as month_log,
    jsonPayload.extra.sourceip as ip, 
    jsonPayLoad.user_id as user_id,
  FROM {{ source('raw', 'stdout') }} 
  WHERE DATE_TRUNC(DATE(timestamp), MONTH) = DATE_TRUNC(DATE('{{ ds() }}'), month) AND
  jsonPayload.extra.route in ('/native/v1/me', '/native/v1/signin') AND
  jsonPayLoad.user_id is not null
), 
ipv4 AS (
    SELECT DISTINCT ip, NET.SAFE_IP_FROM_STRING(ip) AS ip_bytes
    FROM all_ips 
    WHERE BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(ip)) = 4
), ipv4d AS (
    SELECT ip, city_name, country_name, latitude, longitude
    FROM (
        SELECT ip,  ip_bytes & NET.IP_NET_MASK(4, mask) network_bin, mask
        FROM ipv4, UNNEST(GENERATE_ARRAY(8,32)) mask
    )
    JOIN {{ source('raw', 'geoip_city_v4') }}
    USING (network_bin, mask)
    WHERE country_name = "France"
    
)

, ip_aggreg as (
SELECT 
 month_log,
  cast(user_id as string) AS user_id,
  longitude,
  latitude,
  count(*) nb_log_ip
FROM  all_ips ai 
LEFT JOIN ipv4d pv on pv.ip = ai.ip
GROUP BY 1, 2, 3, 4
)

, ip_ranking as (
SELECT 
  *,
  row_number() over(partition by month_log, user_id order by nb_log_ip desc) ranking
FROM ip_aggreg
)


SELECT 
  ip_ranking.month_log,
  ip_ranking.user_id,
  ip_ranking.nb_log_ip,
  iris_france.iriscode as ip_iris,
  iris_france.department 
FROM ip_ranking 
CROSS JOIN {{ source('clean', 'iris_france') }} AS iris_france
WHERE ranking = 1 
AND ST_CONTAINS(iris_france.shape, ST_GEOGPOINT(longitude, latitude))