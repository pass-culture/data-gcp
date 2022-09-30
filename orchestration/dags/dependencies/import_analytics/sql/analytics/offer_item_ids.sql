select
    offer.offer_id,
    CASE
        WHEN (
            offer.offer_subcategoryId IN ('LIVRE_PAPIER')
            AND offer_extracted_data.isbn IS not null
        ) THEN CONCAT('isbn-', offer_extracted_data.isbn)
        WHEN (
            offer.offer_subcategoryId IN ('SEANCE_CINE')
            AND offer_extracted_data.visa IS not null
        ) THEN CONCAT('visa-', offer_extracted_data.visa)
        ELSE CONCAT('product-', offer.offer_product_id)
    END AS item_id,
FROM
    `{{ bigquery_analytics_dataset }}`.applicative_database_offer AS offer
LEFT JOIN 
    `{{ bigquery_analytics_dataset }}`.offer_extracted_data offer_extracted_data ON offer_extracted_data.offer_id = offer.offer_id