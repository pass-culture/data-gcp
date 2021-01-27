SELECT ro.userid, COUNT(DISTINCT o.type) AS typecount
FROM `pass-culture-app-projet-test.data_analytics.offer` AS o
INNER JOIN `pass-culture-app-projet-test.algo_reco_kpi_data.past_recommended_offers` AS ro
ON CAST(ro.offerid as STRING)=o.id
WHERE ro.date >= PARSE_TIMESTAMP('%Y%m%d',@DS_START_DATE)     -- Dates à définir sur la dashboard
AND ro.date < PARSE_TIMESTAMP('%Y%m%d',@DS_END_DATE)          -- pour gérer la période d'AB testing
GROUP BY ro.userid
