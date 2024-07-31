select
    stock.* except (stock_price),
    COALESCE(stock.stock_price, price_category.price) as stock_price,
    price_category.price_category_label_id,
    price_category_label.label as price_category_label
from
    {{ source('raw','applicative_database_stock') }} as stock
    left join {{ source('raw','applicative_database_price_category') }} as price_category on price_category.price_category_id = stock.price_category_id
    left join {{ source('raw','applicative_database_price_category_label') }} as price_category_label on price_category.price_category_label_id = price_category_label.price_category_label_id
