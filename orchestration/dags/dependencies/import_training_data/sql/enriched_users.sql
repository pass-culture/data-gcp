with base as(
    SELECT
        user_id,
        offer_subcategoryid,
        SUM(count) as sub_cat_count
    FROM
        `{{ bigquery_raw_dataset }}`.`training_data_aggregated_users`
    group by
        user_id,
        offer_subcategoryid
), user_total as (
    select
        user_id,
        sum(count) as total_count
    FROM
        `{{ bigquery_raw_dataset }}`.`training_data_aggregated_users`
    group by
        user_id
), user_subcat_prop as(
    select
        b.user_id,
        b.offer_subcategoryid,
        ut.total_count,
        b.sub_cat_count,
        SAFE_DIVIDE(b.sub_cat_count, ut.total_count) as sub_cat_prop
    from
        base b
        join user_total ut on b.user_id = ut.user_id
)
select
    user_id,
    offer_subcategoryid,
    sub_cat_prop,
    sub_cat_count,
    total_count,
    row_number() OVER(
        PARTITION BY user_id
        order by
            sub_cat_prop DESC
    ) as rank_desc,
    row_number() OVER(
        PARTITION BY user_id
        order by
            sub_cat_prop ASC
    ) as rank_asc
from
    user_subcat_prop
order by
    user_id,
    rank_desc,
    rank_asc