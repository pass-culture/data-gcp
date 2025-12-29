{% snapshot snapshot_raw__stock %}

    {{
        config(
            **custom_snapshot_config(
                strategy="check",
                unique_key="stock_id",
                check_cols=[
                    "stock_id_at_providers",
                    "stock_modified_at_last_provider_date",
                    "stock_modified_date",
                    "stock_booking_limit_date",
                    "stock_last_provider_id",
                    "offer_id",
                    "stock_creation_date",
                    "price_category_id",
                    "stock_features",
                ],
                hard_deletes="ignore",
            )
        )
    }}

    select
        stock_id_at_providers,
        stock_modified_at_last_provider_date,
        stock_id,
        stock_price,
        stock_quantity,
        stock_booking_limit_date,
        stock_last_provider_id,
        offer_id,
        stock_is_soft_deleted,
        stock_beginning_date,
        stock_creation_date,
        stock_fields_updated,
        price_category_id,
        stock_features,
        stock_modified_date
    from {{ ref("raw_applicative__stock_lite") }}

{% endsnapshot %}
