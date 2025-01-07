WITH
    -- Step 1: Initial Data Cleaning and Transformation
    base_cleaning AS (
        SELECT
            offer_id,
            offer_subcategoryid,
            rayon AS category_rayon,
            book_editor AS publisher_name,
            REGEXP_REPLACE(ean, r'[\\s\\-\\tA-Za-z]', '') AS cleaned_ean,
            REGEXP_REPLACE(isbn, r'[\\s\\-\\tA-Za-z]', '') AS cleaned_isbn,
            LENGTH(REGEXP_REPLACE(isbn, r'[\\s\\-\\tA-Za-z]', '')) AS isbn_length,
            LENGTH(REGEXP_REPLACE(ean, r'[\\s\\-\\tA-Za-z]', '')) AS ean_length,
            CASE
                WHEN LENGTH(CAST(titelive_gtl_id AS STRING)) = 7
                THEN CONCAT('0', CAST(titelive_gtl_id AS STRING))
                ELSE CAST(titelive_gtl_id AS STRING)
            END AS normalized_gtl_id
        FROM {{ ref("int_applicative__extract_offer") }}
    ),

    -- Step 2: Validate ISBN and EAN
    validate_identifiers AS (
        SELECT
            offer_id,
            offer_subcategoryid,
            cleaned_ean,
            cleaned_isbn,
            isbn_length,
            ean_length,
            normalized_gtl_id,
            category_rayon,
            publisher_name,
            CASE
                WHEN isbn_length = 10 AND REGEXP_CONTAINS(cleaned_isbn, r'^\d{10}$') THEN 'valid'
                WHEN isbn_length = 13 AND REGEXP_CONTAINS(cleaned_isbn, r'^\d{13}$') THEN 'valid'
                WHEN cleaned_isbn IS null THEN 'missing'
                ELSE 'invalid_format'
            END AS isbn_status,
            CASE
                WHEN ean_length = 10 AND REGEXP_CONTAINS(cleaned_ean, r'^\d{10}$') THEN 'valid'
                WHEN ean_length = 13 AND REGEXP_CONTAINS(cleaned_ean, r'^\d{13}$') THEN 'valid'
                WHEN cleaned_ean IS null THEN 'missing'
                ELSE 'invalid_format'
            END AS ean_status
        FROM base_cleaning
    ),

    -- Step 3: Filter Valid ISBN and EAN
    filter_valid_identifiers AS (
        SELECT
            offer_id,
            offer_subcategoryid,
            normalized_gtl_id,
            category_rayon,
            publisher_name,
            ean_length,
            isbn_length,
            CASE WHEN isbn_status = 'valid' THEN cleaned_isbn END AS valid_isbn,
            CASE WHEN ean_status = 'valid' THEN cleaned_ean END AS valid_ean
        FROM validate_identifiers
    ),

    -- Step 4: Coalesce EAN and ISBN into Primary ISBN
    clean_offer_ean_cte AS (
        SELECT
            offer_id,
            offer_subcategoryid,
            category_rayon,
            publisher_name,
            normalized_gtl_id AS titelive_gtl_id,
            valid_ean AS ean,
            IF(ean_length = 13, valid_ean, valid_isbn) AS isbn
        FROM filter_valid_identifiers
    ),

    -- Step 5: Determine the Most Frequent Rayon per ISBN
    determine_rayon_by_isbn AS (
        SELECT
            isbn,
            category_rayon
        FROM clean_offer_ean_cte
        WHERE
            offer_subcategoryid IN ('LIVRE_PAPIER', 'LIVRE_NUMERIQUE', 'LIVRE_AUDIO_PHYSIQUE')
            AND category_rayon IS NOT null
            AND isbn IS NOT null
        GROUP BY isbn, category_rayon
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY isbn
            ORDER BY COUNT(DISTINCT offer_id) DESC, MAX(offer_id) DESC
        ) = 1
    ),

    -- Step 6: Determine the Most Frequent Publisher per ISBN
    determine_editor_by_isbn AS (
        SELECT
            isbn,
            publisher_name
        FROM clean_offer_ean_cte
        WHERE
            offer_subcategoryid IN ('LIVRE_PAPIER', 'LIVRE_NUMERIQUE', 'LIVRE_AUDIO_PHYSIQUE')
            AND publisher_name IS NOT null
            AND isbn IS NOT null
        GROUP BY isbn, publisher_name
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY isbn
            ORDER BY COUNT(DISTINCT offer_id) DESC, MAX(offer_id) DESC
        ) = 1
    )

-- Final Selection of Processed Data
SELECT
    clean_offer_ean_cte.offer_id,
    clean_offer_ean_cte.ean,
    clean_offer_ean_cte.isbn,
    clean_offer_ean_cte.titelive_gtl_id,
    determine_rayon_by_isbn.category_rayon AS rayon,
    determine_editor_by_isbn.publisher_name AS book_editor
FROM clean_offer_ean_cte
LEFT JOIN determine_rayon_by_isbn
    ON clean_offer_ean_cte.isbn = determine_rayon_by_isbn.isbn
LEFT JOIN determine_editor_by_isbn
    ON clean_offer_ean_cte.isbn = determine_editor_by_isbn.isbn
