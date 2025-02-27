{% if is_first_day_of_month() == "TRUE" %}

    select
        * except (offer_updated_date),
        timestamp(offer_updated_date) as offer_updated_date,
        to_hex(md5(to_json_string(offer))) as custom_scd_id
    from {{ source("raw", "applicative_database_offer_legacy") }} as offer

{% else %}

select 1 as dummy_column

{% endif %}
