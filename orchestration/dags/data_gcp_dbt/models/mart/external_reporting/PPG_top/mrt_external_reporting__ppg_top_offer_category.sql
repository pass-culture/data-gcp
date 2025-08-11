{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "execution_date", "data_type": "date"},
            on_schema_change="append_new_columns",
        )
    )
}}

{% set dimensions = [
    {"name": "NAT", "value_expr": "'NAT'"},
    {"name": "REG", "value_expr": "venue_region_name"},
    {"name": "DEP", "value_expr": "venue_department_name"},
] %}

{% for dim in dimensions %}
    {% if not loop.first %}
        union all
    {% endif %}
SELECT
    date_trunc(date(booking_used_date), MONTH) AS execution_date,
    date("{{ ds() }}") as update_date,
    '{{ dim.name }}' as dimension_name,
    {{ dim.value_expr }} as dimension_value,
    item_id,
    offer_category_id,
    offer_subcategory_id,
    ANY_VALUE(offer_name) AS offer_name,
    SUM(booking_intermediary_amount) AS total_booking_amount,
    SUM(booking_quantity) AS total_booking_quantity,
    ROW_NUMBER() OVER(PARTITION BY offer_category_id ORDER BY SUM(booking_intermediary_amount) DESC) as total_booking_amount_ranked
FROM {{ ref('mrt_global__booking') }}
WHERE booking_is_used
{% if is_incremental() %}
    AND date_trunc(date(booking_used_date), MONTH) = date_trunc(date("{{ ds() }}"), month)
{% endif %}
GROUP BY
    execution_date,
    update_date,
    dimension_name,
    dimension_value,
    item_id,
    offer_category_id,
    offer_subcategory_id
ORDER BY
    execution_date,
    dimension_name,
    dimension_value,
    offer_category_id,
    total_booking_amount_ranked,
    total_booking_amount,
    total_booking_quantity

QUALIFY ROW_NUMBER() OVER(PARTITION BY offer_category_id ORDER BY SUM(booking_intermediary_amount) DESC) <= 50
{% endfor %}
