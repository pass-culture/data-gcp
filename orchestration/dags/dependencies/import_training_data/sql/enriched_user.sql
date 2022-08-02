with base as(
    SELECT
        user_id,
        offer_subcategoryid,
        SUM(count) as sub_cat_count
    FROM
        `{{ bigquery_clean_dataset }}`.`training_data_aggregated_users`
    group by
        user_id,
        offer_subcategoryid
    limit
        10
), user_total as (
    select
        user_id,
        sum(count) as total_count
    FROM
        `{{ bigquery_clean_dataset }}`.`training_data_aggregated_users`
    group by
        user_id
    limit
        10
), user_subcat_prop as(
    select
        b.user_id,
        b.offer_subcategoryid,
        SAFE_DIVIDE(b.sub_cat_count, ut.total_count) as sub_cat_prop
    from
        base b
        join user_total ut on b.user_id = ut.user_id
) 
select user_id,offer_subcategoryid,sub_cat_prop from user_subcat_prop