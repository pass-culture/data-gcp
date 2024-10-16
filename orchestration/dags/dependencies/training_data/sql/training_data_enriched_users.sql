with
    base as (
        select user_id, offer_subcategory_id, sum(count) as sub_cat_count
        from `{{ bigquery_clean_dataset }}`.`training_data_aggregated_users`
        group by user_id, offer_subcategory_id
    ),
    user_total as (
        select user_id, sum(count) as total_count
        from `{{ bigquery_clean_dataset }}`.`training_data_aggregated_users`
        group by user_id
    ),
    user_subcat_prop as (
        select
            b.user_id,
            b.offer_subcategory_id,
            ut.total_count,
            b.sub_cat_count,
            safe_divide(b.sub_cat_count, ut.total_count) as sub_cat_prop
        from base b
        join user_total ut on b.user_id = ut.user_id
    )
select
    user_id,
    offer_subcategory_id,
    sub_cat_prop,
    sub_cat_count,
    total_count,
    row_number() over (partition by user_id order by sub_cat_prop desc) as rank_desc,
    row_number() over (partition by user_id order by sub_cat_prop asc) as rank_asc
from user_subcat_prop
order by user_id, rank_desc, rank_asc
