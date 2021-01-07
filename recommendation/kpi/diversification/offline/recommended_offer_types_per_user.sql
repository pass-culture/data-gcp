SELECT ro.userid, COUNT(DISTINCT o.type) AS typecount
FROM `pass-culture-app-projet-test.applicative_database.offer` AS o
JOIN `pass-culture-app-projet-test.algo_reco_kpi_data.past_recommended_offers` AS ro
ON CAST(ro.offerid as STRING)=o.id GROUP BY ro.userid;
