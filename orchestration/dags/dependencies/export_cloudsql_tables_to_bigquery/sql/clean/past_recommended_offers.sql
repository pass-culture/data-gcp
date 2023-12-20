WITH export_table AS (
    SELECT
        id,
        date(date) as event_date,
        cast(userid as string) as user_id,
        cast(offerid as string) as offer_id,
        date,
        group_id,
        reco_origin,
        model_name,
        model_version,
        call_id,
        reco_filters,
        user_iris_id,
        import_date,
        ROW_NUMBER() OVER (
            PARTITION BY 
            offerid,
            userid,
            call_id
            ORDER BY
                date DESC
        ) as row_number
    FROM
        `{{ bigquery_raw_dataset }}.past_recommended_offers`
    WHERE import_date >= DATE('{{ add_days(ds, -365) }}')
)
SELECT
    *
except

(row_number)

FROM
    export_table
WHERE
    row_number = 1
AND 
    import_date between date_sub(current_date, interval 30 day) and current_date