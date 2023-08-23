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
    enriched_item_metadata.subcategory_id AS subcategory_id,
    enriched_item_metadata.category_id as category,
    enriched_item_metadata.offer_type_id,
    enriched_item_metadata.offer_type_label,
    enriched_item_metadata.offer_sub_type_id,
    enriched_item_metadata.offer_sub_type_label,
    offer_extracted_data.author,
    offer_extracted_data.performer,
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
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_offer_data eod on o.offer_id=eod.offer_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.offer_item_ids oii on o.offer_id=oii.offer_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.enriched_item_metadata enriched_item_metadata on oii.item_id = enriched_item_metadata.item_id
    LEFT JOIN mediation ON o.offer_id = mediation.offer_id
    LEFT JOIN `{{ bigquery_analytics_dataset }}`.offer_extracted_data offer_extracted_data ON offer_extracted_data.offer_id = o.offer_id
WHERE
    oii.item_id not in (
        select
            distinct item_id
        from
            `{{ bigquery_clean_dataset }}`.item_embeddings_v2
    ) QUALIFY ROW_NUMBER() OVER (
        PARTITION BY item_id
        ORDER BY
            eod.booking_cnt DESC
    ) = 1
ORDER BY o.offer_creation_date DESC
LIMIT {{ params.batch_size }}
