CREATE TABLE IF NOT EXISTS {{ dataset }}.offer_statistics(
    update_date String,
    item_id     String,
    offer_id        String,
    category        Nullable(String),
    subcategory_id      Nullable(String),
    search_group_name       Nullable(String),
    venue_id        Nullable(String),
    offer_name        Nullable(String),
    gtl_id      Nullable(String),
    gtl_l1      Nullable(String),
    gtl_l2      Nullable(String),
    gtl_l3      Nullable(String),
    gtl_l4      Nullable(String),
    topic_id        Nullable(String),
    cluster_id      Nullable(String),
    is_numerical        Boolean,
    is_national     Boolean,
    is_geolocated       Boolean,
    offer_creation_date     Datetime64,
    stock_beginning_date    Nullable(Datetime64),
    stock_price         Nullable(Float64),
    offer_is_duo        Boolean,
    offer_type_domain       Nullable(String),
    offer_type_label        Nullable(String),
    offer_type_labels       Nullable(String),
    total_offers        Nullable(String),
    booking_number      Int32,
    booking_number_last_7_days      Nullable(Int32),
    booking_number_last_14_days     Nullable(Int32),
    booking_number_last_28_days     Nullable(Int32),
    is_underage_recommendable       Nullable(Int32),
    is_sensitive        Boolean,
    is_restrained       Boolean,
    venue_latitude      Nullable(Float64),
    venue_longitude     Nullable(Float64)
)
  ENGINE = MergeTree
  PARTITION BY partition_date
  ORDER BY tuple(item_id, offer_id)
  COMMENT 'Offer statistics for recommendation purposes'
 