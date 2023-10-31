WITH events_stats AS (
    SELECT
        DISTINCT date(install_time) as install_date,
        app,
        media_source,
    FROM
        `{{ bigquery_clean_dataset }}.appsflyer_users`
),
global_install_stats AS (
    SELECT
        date(date) as install_date,
        app,
        media_source,
        sum(installs) as installs,
        sum(total_cost) as total_costs,
    FROM
        `{{ bigquery_raw_dataset }}.appsflyer_daily_report` ad
    group by
        1,
        2,
        3
),
restricted_campaigns AS (
    SELECT
        au.install_date,
        au.app,
        au.media_source,
    FROM
        global_install_stats au
        LEFT JOIN events_stats es on es.install_date = au.install_date
        AND es.app = au.app
        AND es.media_source = au.media_source
    WHERE
        es.install_date is null
        AND au.media_source not in ("Organic", "af_banner", "User_invite", "QR_code")
        and installs > 0
),
appsflyer_users AS (
    SELECT
        firebase_id,
        appsflyer_id,
        install_time,
        date(install_time) as install_date,
        app,
        media_source,
        campaign,
        campaign_id,
        adset,
        adset_id,
        ad as adgroup,
        ROW_NUMBER() OVER (
            PARTITION BY app,
            date(install_time)
            ORDER BY
                install_time
        ) as rnk,
    FROM
        `{{ bigquery_clean_dataset }}.appsflyer_users`
),
global_detailled_install_stats AS (
    SELECT
        date(adr.date) as install_date,
        adr.app,
        adr.media_source,
        adr.campaign,
        adr.campaign_id,
        adr.adset,
        adr.adset_id,
        adr.adgroup,
        sum(installs) as installs,
        sum(total_cost) as total_campaign_costs,
    FROM
        `{{ bigquery_raw_dataset }}.appsflyer_daily_report` adr
    group by
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8
),
exploded_restricted_stats AS (
    SELECT
        gis.*,
        SAFE_DIVIDE(total_campaign_costs, installs) as cost,
        ROW_NUMBER() OVER (
            PARTITION BY gis.install_date,
            gis.app
            ORDER BY
                total_campaign_costs DESC
        ) AS rnk
    FROM
        global_detailled_install_stats gis
        INNER JOIN restricted_campaigns rc on rc.app = gis.app
        AND rc.install_date = gis.install_date
        AND rc.media_source = gis.media_source
        CROSS JOIN UNNEST(GENERATE_ARRAY(1, installs)) AS rnk
),
predicted_appsflyer_user AS (
    SELECT
        "predicted" as appsflyer_user_type,
        firebase_id,
        appsflyer_id,
        install_time,
        es.install_date,
        es.app,
        es.media_source,
        es.campaign,
        es.campaign_id,
        es.adset,
        es.adset_id,
        es.adgroup,
        es.cost as cost_per_install,
        es.total_campaign_costs,
        es.installs as total_campaign_installs
    FROM
        exploded_restricted_stats es
        INNER JOIN appsflyer_users au ON au.rnk = es.rnk
        AND au.install_date = es.install_date
        AND au.app = es.app
),
others_appsflyer_user AS (
    SELECT
        "real" as appsflyer_user_type,
        au.firebase_id,
        au.appsflyer_id,
        au.install_time,
        au.install_date,
        au.app,
        au.media_source,
        au.campaign,
        au.campaign_id,
        au.adset,
        au.adset_id,
        au.adgroup,
        SAFE_DIVIDE(costs.total_campaign_costs, costs.installs) AS cost_per_install,
        costs.total_campaign_costs,
        costs.installs as total_campaign_installs
    FROM
        appsflyer_users au
        LEFT JOIN predicted_appsflyer_user pau ON au.firebase_id = pau.firebase_id
        AND au.appsflyer_id = pau.appsflyer_id
        AND au.install_date = pau.install_date
        AND au.app = pau.app
        LEFT JOIN global_detailled_install_stats costs ON au.app = costs.app
        AND au.install_date = costs.install_date
        AND au.adset = costs.adset
        AND au.adgroup = costs.adgroup
        AND au.adset_id = costs.adset_id
        AND au.media_source = costs.media_source
        AND au.campaign = costs.campaign
        AND au.campaign_id = costs.campaign_id
    WHERE
        pau.firebase_id is null
)
SELECT
    appsflyer_user_type,
    firebase_id,
    appsflyer_id,
    install_time,
    install_date,
    app,
    media_source,
    campaign,
    campaign_id,
    adset,
    adset_id,
    adgroup,
    cost_per_install,
    total_campaign_costs,
    total_campaign_installs
FROM
    predicted_appsflyer_user
UNION
ALL
SELECT
    appsflyer_user_type,
    firebase_id,
    appsflyer_id,
    install_time,
    install_date,
    app,
    media_source,
    campaign,
    campaign_id,
    adset,
    adset_id,
    adgroup,
    cost_per_install,
    total_campaign_costs,
    total_campaign_installs
FROM
    others_appsflyer_user