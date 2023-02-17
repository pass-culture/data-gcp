SELECT
    EAN13 AS isbn
    , POIDS AS book_weight
    , LONGUEUR AS book_length
    , LARGEUR AS book_width
FROM `{{ bigquery_raw_dataset }}.titelive_isbn_weight`