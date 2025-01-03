SELECT
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
FROM {{ ref("int_appsflyer__cost_report") }} AS cost_report
INNER JOIN {{ ref("int_appsflyer__cohort_report") }} AS cohort_report ON cost_report.install_date = cohort_report.install_date AND cost_report.app_id = cohort_report.app_id AND cost_report.media_source = cohort_report.media_source AND cost_report.campaign = cohort_report.campaign AND cost_report.adset = cohort_report.adset AND cost_report.ad = cohort_report.ad
ORDER BY cost_report.install_date DESC
