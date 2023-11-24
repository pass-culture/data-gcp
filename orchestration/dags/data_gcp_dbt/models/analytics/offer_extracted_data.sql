WITH extracted_offers AS (
    SELECT
        offer_id,
        offer_subcategoryid,
        LOWER(
            TRIM(
                JSON_EXTRACT_SCALAR(offer_extra_data, "$.author"),
                " "
            )
        ) AS author,
        LOWER(
            TRIM(
                JSON_EXTRACT_SCALAR(offer_extra_data, "$.performer"),
                " "
            )
        ) AS performer,
        LOWER(
            TRIM(
                JSON_EXTRACT_SCALAR(offer_extra_data, "$.musicType"),
                " "
            )
        ) AS musicType,
        LOWER(
            TRIM(
                JSON_EXTRACT_SCALAR(offer_extra_data, "$.musicSubtype"),
                " "
            )
        ) AS musicSubtype,
        LOWER(
            TRIM(
                JSON_EXTRACT_SCALAR(offer_extra_data, "$.stageDirector"),
                " "
            )
        ) AS stageDirector,
        LOWER(
            TRIM(
                JSON_EXTRACT_SCALAR(offer_extra_data, "$.showType"),
                " "
            )
        ) AS showType,
        LOWER(
            TRIM(
                JSON_EXTRACT_SCALAR(offer_extra_data, "$.showSubType"),
                " "
            )
        ) AS showSubType,
        LOWER(
            TRIM(
                JSON_EXTRACT_SCALAR(offer_extra_data, "$.speaker"),
                " "
            )
        ) AS speaker,
        LOWER(
            TRIM(
                JSON_EXTRACT_SCALAR(offer_extra_data, "$.rayon"),
                " "
            )
        ) AS rayon,
        LOWER(
            TRIM(
                JSON_EXTRACT_SCALAR(offer_extra_data, "$.theater.allocine_movie_id"),
                " "
            )
        ) AS theater_movie_id,
        LOWER(
            TRIM(
                JSON_EXTRACT_SCALAR(offer_extra_data, "$.theater.allocine_room_id"),
                " "
            )
        ) AS theater_room_id,
        LOWER(
            TRIM(
                JSON_EXTRACT_SCALAR(offer_extra_data, "$.type"),
                " "
            )
        ) AS movie_type,
        LOWER(
            TRIM(
                JSON_EXTRACT_SCALAR(offer_extra_data, "$.visa"),
                " "
            )
        ) AS visa,
        LOWER(
            TRIM(
                JSON_EXTRACT_SCALAR(offer_extra_data, "$.releaseDate"),
                " "
            )
        ) AS releaseDate,
        LOWER(
            TRIM(JSON_EXTRACT(offer_extra_data, "$.genres"), " ")
        ) AS genres,
        LOWER(
            TRIM(
                JSON_EXTRACT(offer_extra_data, "$.companies"),
                " "
            )
        ) AS companies,
        LOWER(
            TRIM(
                JSON_EXTRACT(offer_extra_data, "$.countries"),
                " "
            )
        ) AS countries,
        LOWER(
            TRIM(
            JSON_EXTRACT(offer_extra_data, "$.cast"),
            " "
            )
        ) AS casting,
        LOWER(
            TRIM(TRIM(
            JSON_EXTRACT(offer_extra_data, "$.isbn"), " "),
            '"')
        ) AS isbn,
        LOWER(
            TRIM(TRIM(
                JSON_EXTRACT(offer_extra_data, "$.ean"), " "),
                '"')
        ) AS ean,
        LOWER(
            TRIM(TRIM(
            JSON_EXTRACT(offer_extra_data, "$.editeur"), " "),
            '"')
        ) AS book_editor,
        TRIM(
            TRIM(
                JSON_EXTRACT(offer_extra_data, "$.gtl_id"), " "),
            '"') AS titelive_gtl_id
    FROM  {{ ref('applicative_database_offer') }}

)

SELECT 
    * EXCEPT(isbn, titelive_gtl_id), 
    if(length(ean) = 13, COALESCE(ean, isbn), isbn) as isbn,
    CASE
      WHEN LENGTH(cast(titelive_gtl_id as string) ) = 7 THEN CONCAT('0', cast(titelive_gtl_id as string) )
      ELSE cast(titelive_gtl_id as string) 
    END AS titelive_gtl_id

FROM extracted_offers