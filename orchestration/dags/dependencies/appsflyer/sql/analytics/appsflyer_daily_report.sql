with
    inapp_events as (
        select
            date(event_date) as event_date,
            case
                when app_id = "id1557887412"
                then "ios"
                when app_id = "app.passculture.webapp"
                then "android"
                else "unknown"
            end as app,
            media_source as media_source,
            campaign as campaign,
            adset as adset,
            ad,
            sum(
                if(event_name = "af_conversion", cast(event_count as float64), 0)
            ) as total_installs,
            sum(
                if(
                    event_name = "af_complete_beneficiary",
                    cast(unique_users as float64),
                    0
                )
            ) as total_beneficiaries,
            sum(
                if(
                    event_name = "af_complete_registration",
                    cast(unique_users as float64),
                    0
                )
            ) as total_registrations,
            sum(
                if(event_name = "af_open_app", cast(unique_users as float64), 0)
            ) as total_open_app,
            sum(
                if(
                    event_name = "af_complete_book_offer",
                    cast(unique_users as float64),
                    0
                )
            ) as total_bookings,

        from `{{ bigquery_appsflyer_import_dataset }}.cohort_user_acquisition` adr
        where conversion_type = "install"
        group by 1, 2, 3, 4, 5, 6
    ),

    global_stats as (
        select
            date(dr.event_date) as event_date,
            dr.app,
            if(dr.media_source = "Organic", "organic", dr.media_source) as media_source,
            if(dr.campaign = "nan", null, dr.campaign) as campaign,
            if(dr.adset = "nan", null, dr.adset) as adset,
            if(dr.adgroup = "nan", null, dr.adgroup) as ad,
            sum(impressions) as impressions,
            sum(clicks) as clicks,
            sum(installs) as installs,
            sum(total_cost) as total_cost,

        from `{{ bigquery_raw_dataset }}.appsflyer_daily_report` dr
        group by 1, 2, 3, 4, 5, 6
    )

select
    ie.event_date,
    ie.app,
    ie.media_source,
    ie.campaign,
    ie.adset,
    ie.ad,
    sum(impressions) as impressions,
    sum(clicks) as clicks,
    sum(installs) as installs,
    sum(total_cost) as total_cost,
    sum(coalesce(total_registrations, 0)) as total_registrations,
    sum(coalesce(total_beneficiaries, 0)) as total_beneficiaries,
    sum(coalesce(total_installs, 0)) as total_installs,
    sum(coalesce(total_open_app, 0)) as total_open_app,
    sum(coalesce(total_bookings, 0)) as total_bookings
from inapp_events ie
left join
    global_stats dr
    on date(dr.event_date) = ie.event_date
    and dr.app = ie.app
    and coalesce(dr.media_source, "-1") = coalesce(ie.media_source, "-1")
    and coalesce(dr.campaign, "-1") = coalesce(ie.campaign, "-1")
    and coalesce(dr.adset, "-1") = coalesce(ie.adset, "-1")
    and coalesce(dr.ad, "-1") = coalesce(ie.ad, "-1")
group by 1, 2, 3, 4, 5, 6
