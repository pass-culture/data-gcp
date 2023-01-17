SELECT
    *
except
(
        row_number,
        ending_datetime,
        beginning_datetime,
        is_geolocated,
        is_duo,
        is_event,
        is_thing,
        price_max,
        min_offers
    ),
    TIMESTAMP(ending_datetime) AS ending_datetime,
    TIMESTAMP(beginning_datetime) AS beginning_datetime,
    CAST(is_geolocated AS bool) AS is_geolocated,
    CAST(is_duo AS bool) AS is_duo,
    CAST(is_event AS bool) AS is_event,
    CAST(is_thing AS bool) AS is_thing,
    CAST(price_max AS FLOAT64) AS price_max,
    CAST(min_offers AS INT64) AS min_offers
FROM
    (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY id
                ORDER BY
                    execution_date DESC
            ) as row_number
        FROM
            `{{ bigquery_raw_dataset }}.contentful_entries`
    ) inn
WHERE
    row_number = 1