WITH all_ips AS (
  SELECT 
    distinct jsonPayload.extra.sourceip as ip, 
    jsonPayLoad.user_id,
  FROM `{{ bigquery_raw_dataset }}.stdout` 
  WHERE DATE_TRUNC(DATE(timestamp), MONTH) = DATE_TRUNC(DATE('{{ ds }}'), month)
  and jsonPayload.extra.route in ('/native/v1/me', '/native/v1/signin')
), 
ipv4 AS (
    SELECT DISTINCT ip, NET.SAFE_IP_FROM_STRING(ip) AS ip_bytes, user_id
    FROM all_ips 
    WHERE BYTE_LENGTH(NET.SAFE_IP_FROM_STRING(ip)) = 4
), ipv4d AS (
    SELECT ip, user_id, city_name, country_name, latitude, longitude
    FROM (
        SELECT ip, user_id, ip_bytes & NET.IP_NET_MASK(4, mask) network_bin, mask
        FROM ipv4, UNNEST(GENERATE_ARRAY(8,32)) mask
    )
    JOIN `{{ bigquery_raw_dataset }}.geoip_city_v4`
    USING (network_bin, mask)
    WHERE country_name = "France"
    AND user_id is not null
)

, ip_aggreg as (
SELECT 
  DATE_TRUNC(DATE('{{ ds }}'), month) AS month_log,
  cast(user_id as string) AS user_id,
  longitude,
  latitude,
  count(*) nb_log_ip
FROM ipv4d
GROUP BY 1, 2, 3, 4)

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
CROSS JOIN `{{ bigquery_clean_dataset }}.iris_france`
WHERE ranking = 1 
AND ST_CONTAINS(iris_france.shape, ST_GEOGPOINT(longitude, latitude))