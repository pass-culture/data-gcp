{{ create_humanize_id_function() }} WITH offer_humanized_id AS (
    SELECT
        offer_id,
        humanize_id(offer_id) AS humanized_id,
    FROM
        `{{ bigquery_analytics_dataset }}.`.applicative_database_offer
    WHERE
        offer_id is not NULL
),
mediation AS (
    SELECT
        offer_id,
        humanize_id(id) as mediation_humanized_id
    FROM
        (
            SELECT
                id,
                offerId as offer_id,
                ROW_NUMBER() OVER (
                    PARTITION BY offerId
                    ORDER BY
                        dateModifiedAtLastProvider DESC
                ) as rnk
            FROM
                `{{ bigquery_analytics_dataset }}`.applicative_database_mediation
            WHERE
                isActive
        ) inn
    WHERE
        rnk = 1
)
SELECT
    o.offer_id,
    oii.item_id,
    CASE
        when (
            o.offer_name is null
            or o.offer_name = 'NaN'
        ) then "None"
        else safe_cast(o.offer_name as STRING)
    END as offer_name,
    CASE
        when (
            o.offer_description is null
            or o.offer_description = 'NaN'
        ) then "None"
        else safe_cast(o.offer_description as STRING)
    END as offer_description,
    o.offer_subcategoryid,
    CASE
        WHEN mediation.mediation_humanized_id is not null THEN CONCAT(
            "https://storage.googleapis.com/{{ mediation_url }}-assets-fine-grained/thumbs/mediations/",
            mediation.mediation_humanized_id
        )
        ELSE CONCAT(
            "https://storage.googleapis.com/{{ mediation_url }}-assets-fine-grained/thumbs/products/",
            humanize_id(o.offer_product_id)
        )
    END AS image_url,
FROM
    `{{ bigquery_raw_dataset }}`.applicative_database_offer o
    LEFT JOIN mediation ON o.offer_id = mediation.offer_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.offer_item_ids oii on o.offer_id = oii.offer_id
    LEFT join `{{ bigquery_analytics_dataset }}`.enriched_offer_data eod on eod.offer_id = o.offer_id
WHERE
    oii.item_id not in (
        select
            distinct item_id
        from
            `{{ bigquery_clean_dataset }}`.item_embeddings
        WHERE
            date(extraction_date) > DATE_SUB(CURRENT_DATE, INTERVAL 60 DAY)
    ) QUALIFY ROW_NUMBER() OVER (
        PARTITION BY item_id,
        offer_id
        ORDER BY
            eod.booking_cnt DESC
    ) = 1
LIMIT {{ params.batch_size }}