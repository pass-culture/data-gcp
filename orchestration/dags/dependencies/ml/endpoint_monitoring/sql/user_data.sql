-- get_user_data.sql
with
    user_sample as (
        select distinct fe.user_id
        from `{{ bigquery_analytics_dataset }}.native_event` fe
        where
            event_date > date_sub(current_date(), interval 2 month)
            and event_name = 'ConsultOffer'
            and user_id is not null
        limit 1000
    ),
    users_clics as (
        select us.user_id, cast(fe.offer_id as string) as offer_id
        from user_sample us
        join
            `{{ bigquery_analytics_dataset }}.native_event` fe
            on fe.user_id = us.user_id
        where
            event_date > date_sub(current_date(), interval 2 month)
            and event_name = 'ConsultOffer'
    ),
    base_booking as (
        select eud.user_id, cast(ebd.offer_id as string) as offer_id
        from (select distinct user_id from user_sample) eud
        join
            `{{ bigquery_analytics_dataset }}.global_booking` ebd
            on ebd.user_id = eud.user_id
    ),
    base_interaction as (
        select *, "booking" as event_name
        from base_booking
        union all
        select *, "clics" as event_name
        from users_clics
    ),
    enriched_interaction as (
        select
            bi.user_id,
            eom.offer_subcategory_id,
            eom.offer_type_label,
            eom.offer_sub_type_label,
            event_name,
            count(*) as subcat_count
        from base_interaction bi
        join
            `{{ bigquery_analytics_dataset }}.global_offer_metadata` eom
            on eom.offer_id = bi.offer_id
        where
            eom.offer_subcategory_id
            in ("LIVRE_PAPIER", "SUPPORT_PHYSIQUE_MUSIQUE_CD", "SEANCE_CINE")
        group by
            bi.user_id,
            offer_subcategory_id,
            eom.offer_type_label,
            eom.offer_sub_type_label,
            event_name
    ),
    user_subcategory_count as (
        select user_id, offer_subcategory_id, sum(subcat_count) as total_count
        from enriched_interaction
        group by user_id, offer_subcategory_id
    ),
    user_total_count as (
        select user_id, sum(total_count) as total_bookings
        from user_subcategory_count
        group by user_id
    ),
    user_top_subcategory as (
        select
            us.user_id,
            us.offer_subcategory_id,
            us.total_count,
            ut.total_bookings,
            (us.total_count / ut.total_bookings) as subcategory_ratio
        from user_subcategory_count us
        join user_total_count ut on us.user_id = ut.user_id
        where
            (us.total_count / ut.total_bookings) >= 0.5
            and us.offer_subcategory_id
            in ("LIVRE_PAPIER", "SUPPORT_PHYSIQUE_MUSIQUE_CD", "SEANCE_CINE")
    ),
    ranked_users as (
        select
            uts.user_id,
            uts.offer_subcategory_id,
            uts.total_count,
            uts.subcategory_ratio,
            row_number() over (
                partition by uts.offer_subcategory_id order by uts.total_count desc
            ) as rank
        from user_top_subcategory uts
    )
select ru.user_id, ru.offer_subcategory_id, ru.total_count, ru.subcategory_ratio
from ranked_users ru
where ru.rank <= 1000
order by ru.offer_subcategory_id asc, ru.total_count desc
;
