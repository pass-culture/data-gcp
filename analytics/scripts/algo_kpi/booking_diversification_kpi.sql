WITH actions AS (
    select CAST(userId AS INT64) as userId , offerId, CAST(dateCreated AS TIMESTAMP) as action_date
    from (select userId, stockId, dateCreated from `pass-culture-app-projet-test.poc_data_federated_query.booking`) a
    left join (select id, offerId from `pass-culture-app-projet-test.poc_data_federated_query.stock`) b
    on CAST(a.stockId AS INT64) = b.id
),
scrolls AS (
    select 100000000000 + ROW_NUMBER() OVER() as scroll_date, 2732 AS user_id # select scrolls.server_time as scroll_date, user_id_dehumanized as user_id
    from (select idvisit, server_time from `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_link_visit_action` limit 1000) scrolls
    left join (select idvisit, user_id_dehumanized as user_id from `pass-culture-app-projet-test.algo_reco_kpi_matomo.log_visit`) visits
    on scrolls.idvisit = visits.idvisit
),
# recos AS (select userid as userId, offerid as offerId, date as reco_date from `pass-culture-app-projet-test.algo_reco_kpi_data.past_recommended_offers`),
recos AS (
    select userId, offerId, TIMESTAMP_ADD(action_date, INTERVAL -1 MINUTE) as reco_date from actions limit 100
),
viewed_recos AS (
        select * from (
        select userId, offerId, reco_date , scroll_date,
        RANK() OVER (PARTITION BY userId, reco_date ORDER BY TIMESTAMP_DIFF(reco_date, TIMESTAMP_SECONDS(scroll_date), SECOND)) AS time_rank
        from recos left join scrolls
        on recos.userId = scrolls.user_id
        where TIMESTAMP_SECONDS(scroll_date) >= reco_date
    ) where time_rank = 1
),
offers AS (
    select id as offerId, type as offerType, url from `pass-culture-app-projet-test.poc_data_federated_query.offer`
)

select TIMESTAMP_DIFF(actions.action_date, viewed_recos.reco_date, SECOND) as time_between_reco_and_action, viewed_recos.userId, viewed_recos.offerId, offerType, url
from viewed_recos left join actions
on viewed_recos.userId = actions.userId and viewed_recos.offerId = actions.offerId
left join offers
on viewed_recos.offerId = offers.offerId
where TIMESTAMP_DIFF(actions.action_date, viewed_recos.reco_date, SECOND) >= 0;
