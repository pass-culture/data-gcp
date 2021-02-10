SELECT ro.userid, COUNT(DISTINCT o.offer_type) AS typecount
FROM `passculture-data-prod.analytics_prod.applicative_database_offer` AS o
INNER JOIN `passculture-data-prod.raw_prod.past_recommended_offers` AS ro
ON CAST(ro.offerid as STRING)=o.offer_id
WHERE ro.date >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
AND ro.date < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)          -- pour gérer la période d'AB testing
GROUP BY ro.userid
