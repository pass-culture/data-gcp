SELECT
    stock.* EXCEPT(stock_price),
    COALESCE(stock.stock_price, price_category.price) AS stock_price,
    price_category.price_category_label_id,
    price_category_label.label AS price_category_label
FROM
    `{{ bigquery_analytics_dataset }}`.applicative_database_stock AS stock
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_price_category AS price_category ON price_category.price_category_id = stock.price_category_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.applicative_database_price_category_label AS price_category_label ON price_category.price_category_label_id = price_category_label.price_category_label_id