WITH matching_isbn_with_rayon AS (
SELECT
    isbn
  , rayon
  , ROW_NUMBER() OVER (PARTITION BY isbn ORDER BY COUNT(DISTINCT offer_id) DESC) AS rank_rayon
FROM `{{ bigquery_analytics_dataset }}`.offer_extracted_data
WHERE offer_subcategoryId IN ('LIVRE_PAPIER', 'LIVRE_NUMERIQUE', 'LIVRE_AUDIO_PHYSIQUE')
AND rayon IS NOT NULL
AND isbn IS NOT NULL
GROUP BY 1,2
QUALIFY ROW_NUMBER() OVER (PARTITION BY isbn ORDER BY COUNT(DISTINCT offer_id) DESC) = 1
),

matching_isbn_with_editor AS (
SELECT
    isbn
    , book_editor
    , ROW_NUMBER() OVER (PARTITION BY isbn ORDER BY COUNT(DISTINCT offer_id) DESC) AS rank_editor
FROM `{{ bigquery_analytics_dataset }}`.offer_extracted_data
WHERE offer_subcategoryId IN ('LIVRE_PAPIER', 'LIVRE_NUMERIQUE', 'LIVRE_AUDIO_PHYSIQUE')
AND book_editor IS NOT NULL
AND isbn IS NOT NULL
GROUP BY 1,2
QUALIFY ROW_NUMBER() OVER (PARTITION BY isbn ORDER BY COUNT(DISTINCT offer_id) DESC) = 1
)

SELECT DISTINCT
    isbn
    , rayon
    , book_editor
FROM matching_isbn_with_rayon
LEFT JOIN matching_isbn_with_editor USING(isbn)