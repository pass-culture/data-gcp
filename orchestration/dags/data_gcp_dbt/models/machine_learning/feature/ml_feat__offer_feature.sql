with
    iris_data as (
        select id as iris_id, centroid from {{ ref("int_seed__iris_france") }}
    ),

    offers as (
        select
            offer_id,
            item_id,
            offer_category_id,
            offer_subcategory_id,
            offer_url is null as is_geolocated,
            date_diff(
                current_date(), offer_creation_date, day
            ) as offer_created_delta_in_days


        from {{ ref("int_global__offer") }}
    ),

    stocks as (
        select offer_id, stock_price, stock_beginning_date
        from {{ ref("int_global__stock") }}
        where
            stock_price is not null
            and stock_booking_limit_date
            >= date_sub(date("{{ ds() }}"), interval 28 day)
    ),

    stock_aggregations as (
        select
            offer_id,
            avg(stock_price) as offer_mean_stock_price,
            max(
                date_diff(date("{{ ds() }}"), stock_beginning_date, day)
            ) as offer_max_stock_beginning_days
        from stocks
        group by offer_id
    )

select
    offers.offer_id,
    offers.item_id,
    offers.offer_category_id,
    offers.offer_subcategory_id,
    offers.is_geolocated,
    offers.offer_created_delta_in_days,
    stock_aggregations.offer_mean_stock_price,
    stock_aggregations.offer_max_stock_beginning_days,
    iris_data.iris_id as offer_iris_id,
    iris_data.centroid as offer_centroid,
    st_x(iris_data.centroid) as offer_centroid_x,
    st_y(iris_data.centroid) as offer_centroid_y

from offers
left join stock_aggregations on offers.offer_id = stock_aggregations.offer_id
left join iris_data on offers.offer_id = iris_data.iris_id
