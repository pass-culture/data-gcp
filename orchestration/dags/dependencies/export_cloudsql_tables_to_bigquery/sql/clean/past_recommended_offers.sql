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
            PARTITION BY id,
            userid,
            call_id
            ORDER BY
                date DESC
        ) as row_number
    FROM
        `{{ bigquery_raw_dataset }}.past_recommended_offers`
    WHERE import_date >= DATE_SUB(date('{{ ds }}'), INTERVAL 12 MONTH)
)
SELECT
    *
except

(row_number)

FROM
    export_table
WHERE
    row_number = 1