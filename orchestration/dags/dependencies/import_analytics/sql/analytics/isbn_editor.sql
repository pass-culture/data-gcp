WITH matching_isbn_with_editor AS (
SELECT

    json_extract(offer_extra_data, '$.editeur') AS editeur
    ,json_extract(offer_extra_data, '$.isbn') AS isbn
    , COUNT(DISTINCT offer_id) AS nb_offer
FROM `{{ bigquery_analytics_dataset }}`.applicative_database_offer
WHERE offer_subcategoryId = 'LIVRE_PAPIER'
AND json_extract(offer_extra_data, '$.editeur') IS NOT NULL
AND json_extract(offer_extra_data, '$.isbn') IS NOT NULL
GROUP BY 1,2
),


editeur_per_isbn AS (
SELECT
    isbn
    ,editeur
    , ROW_NUMBER() OVER(PARTITION BY isbn ORDER BY nb_offer DESC) AS rank_nb_offer
FROM matching_isbn_with_editor
)

SELECT
    TRIM(isbn, '"') AS isbn
    , TRIM(editeur, '"') AS book_editor
FROM editeur_per_isbn
WHERE rank_nb_offer = 1