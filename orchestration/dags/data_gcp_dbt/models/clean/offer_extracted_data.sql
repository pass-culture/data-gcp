WITH extra_data as (
  SELECT 
  offer.offer_id,
  COALESCE(product.product_extra_data,offer.offer_extra_data) as extra_data
  FROM {{ source('raw', 'applicative_database_offer') }} AS offer
  LEFT JOIN {{ ref('int_applicative__product') }}  AS product on CAST(product.id as string)=offer.offer_product_id
),
extracted_offers AS (
SELECT 
offer.offer_id,
offer_subcategoryid,
LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.author"), " ")) AS author,
LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.performer"), " ")) AS performer,
LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.musicType"), " ")) AS musicType,
LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.musicSubtype"), " ")) AS musicSubtype,
LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.stageDirector"), " ")) AS stageDirector,
LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.showType"), " ")) AS showType,
LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.showSubType"), " ")) AS showSubType,
LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.speaker"), " ")) AS speaker,
LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.rayon"), " ")) AS rayon,
LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.theater.allocine_movie_id"), " ")) AS theater_movie_id,
LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.theater.allocine_room_id"), " ")) AS theater_room_id,
LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.type"), " ")) AS movie_type,
LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.visa"), " ")) AS visa,
LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.releaseDate"), " ")) AS releaseDate,
LOWER(TRIM(JSON_EXTRACT(extra_data, "$.genres"), " ")) AS genres,
LOWER(TRIM(JSON_EXTRACT(extra_data, "$.companies"), " ")) AS companies,
LOWER(TRIM(JSON_EXTRACT(extra_data, "$.countries"), " ")) AS countries,
LOWER(TRIM(JSON_EXTRACT(extra_data, "$.cast"), " ")) AS casting,
LOWER(TRIM(TRIM(JSON_EXTRACT(extra_data, "$.isbn")," "),'"')) AS isbn,
LOWER(TRIM(TRIM(JSON_EXTRACT(extra_data, "$.ean"), " "),'"')) AS ean,
LOWER(TRIM(TRIM(JSON_EXTRACT(extra_data, "$.editeur")," "),'"')) AS book_editor,
LOWER(TRIM(TRIM(JSON_EXTRACT(extra_data, "$.gtl_id")," "),'"')) AS titelive_gtl_id
FROM {{ source('raw', 'applicative_database_offer') }} offer
LEFT JOIN extra_data AS ued on ued.offer_id=offer.offer_id
)
SELECT
    * EXCEPT(isbn, titelive_gtl_id),
    IF(LENGTH(ean) = 13, COALESCE(ean, isbn), isbn) as isbn,
    CASE
      WHEN LENGTH(cast(titelive_gtl_id as string) ) = 7 THEN CONCAT('0', cast(titelive_gtl_id as string) )
      ELSE cast(titelive_gtl_id as string)
    END AS titelive_gtl_id
FROM extracted_offers
