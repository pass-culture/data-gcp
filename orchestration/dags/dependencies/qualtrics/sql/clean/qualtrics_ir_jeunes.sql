with
    user_visits as (
        select
            user_id,
            count(distinct concat(user_pseudo_id, session_id)) as total_visit_last_month
        from `{{ bigquery_int_firebase_dataset }}.native_event`
        where date(event_date) >= date_sub(current_date, interval 1 month)
        group by user_id
    ),

    previous_export as (
        select distinct user_id
        from `{{ bigquery_clean_dataset }}.qualtrics_ir_jeunes`
        where
            calculation_month
            >= date_sub(date("{{ current_month(ds) }}"), interval 6 month)

    ),

    answers as (
        select distinct user_id from `{{ bigquery_raw_dataset }}.qualtrics_answers`
    ),

    ir_export as (
        select
            user_data.user_id,
            user_data.current_deposit_type as deposit_type,
            user_data.user_civility,
            user_data.total_non_cancelled_individual_bookings as no_cancelled_booking,
            user_data.user_region_name,
            user_data.total_actual_amount_spent as actual_amount_spent,
            user_data.user_activity,
            user_visits.total_visit_last_month, -- TODO legacy: rename field in qualtrics
            user_location.user_rural_city_type as geo_type,
            user_location.qpv_code as code_qpv, -- TODO legacy: rename field in qualtrics
            user_location.zrr_level as zrr,
            user_data.user_seniority

        from `{{ bigquery_analytics_dataset }}.global_user` user_data
        left join
            `{{ bigquery_int_geo_dataset }}.user_location` user_location
            on user_location.user_id = user_data.user_id
        left join
            `{{ bigquery_raw_dataset }}.qualtrics_opt_out_users` opt_out
            on opt_out.ext_ref = user_data.user_id
        left join user_visits on user_data.user_id = user_visits.user_id
        left join answers on user_data.user_id = answers.user_id
        where
            user_data.user_id is not null
            and user_data.current_deposit_type in ("GRANT_15_17", "GRANT_18")
            and user_is_current_beneficiary is true
            and user_data.user_is_active is true
            and user_data.user_has_enabled_marketing_email is true
            and opt_out.contact_id is null
            and answers.user_id is null
    ),

    grant_15_17 as (
        select ir.*
        from ir_export ir
        left join previous_export pe on pe.user_id = ir.user_id
        where ir.deposit_type = "GRANT_15_7" and pe.user_id is null
        order by rand()
        limit {{ params.volume }}
    ),

    grant_18 as (
        select ir.*
        from ir_export ir
        left join previous_export pe on pe.user_id = ir.user_id
        where deposit_type = "GRANT_18" and pe.user_id is null
        order by rand()
        limit {{ params.volume }}
    )

select
    date("{{ current_month(ds) }}") as calculation_month, current_date as export_date, *
from grant_18
union all
select
    date("{{ current_month(ds) }}") as calculation_month, current_date as export_date, *
from grant_15_17
