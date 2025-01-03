select
    cost_report.install_date,
    cost_report.os,
    cost_report.media_source,
    cost_report.campaign,
    cost_report.adset,
    cost_report.ad,
    cost_report.total_costs,
    cost_report.total_installs,
    cohort_report.total_registrations,
    cohort_report.total_beneficiaries_underage,
    cohort_report.total_beneficiaries_18,
    cohort_report.total_beneficiaries_17,
    cohort_report.total_beneficiaries_16,
    cohort_report.total_beneficiaries_15
from {{ ref("int_appsflyer__cost_report") }} as cost_report
inner join
    {{ ref("int_appsflyer__cohort_report") }} as cohort_report
    on cost_report.install_date = cohort_report.install_date
    and cost_report.app_id = cohort_report.app_id
    and cost_report.media_source = cohort_report.media_source
    and cost_report.campaign = cohort_report.campaign
    and cost_report.adset = cohort_report.adset
    and cost_report.ad = cohort_report.ad
order by cost_report.install_date desc
