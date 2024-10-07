WITH base_offers AS (
    SELECT
        eod.offer_id,
        eod.item_id,
        eod.offer_subcategory_id,
        evd.venue_type_label,
        evd.offerer_name
    FROM
        `{{ bigquery_analytics_dataset }}.global_offer` eod
        JOIN `{{ bigquery_raw_dataset }}.applicative_database_offer` o USING (offer_id)
        JOIN `{{ bigquery_analytics_dataset }}.global_venue` evd ON evd.venue_managing_offerer_id = eod.offerer_id
    WHERE
        DATE(eod.offer_creation_date) >= DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH)
        AND eod.offer_validation = "APPROVED"
        AND eod.offer_product_id IS NULL
        AND eod.item_id NOT LIKE 'isbn-%'
    GROUP BY
        1, 2, 3, 4, 5
)
SELECT
    b.*,
    ie.name_embedding AS name_embedding,
    ie.description_embedding AS description_embedding
FROM
    base_offers b
    JOIN (
        SELECT
            item_id,
            name_embedding AS name_embedding,
            description_embedding AS description_embedding
        FROM
            `{{ bigquery_ml_preproc_dataset }}.item_embedding_extraction`
        WHERE
            DATE(extraction_date) >= DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH)
        QUALIFY ROW_NUMBER() OVER (PARTITION BY item_id ORDER BY extraction_date DESC) = 1
    ) ie USING (item_id)
