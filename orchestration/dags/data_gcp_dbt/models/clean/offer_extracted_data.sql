with extra_data as (
    select
        offer.offer_id,
        COALESCE(product.product_extra_data, offer.offer_extra_data) as extra_data
    from {{ source('raw', 'applicative_database_offer') }} as offer
        left join {{ ref('int_applicative__product') }} as product on CAST(product.id as string) = offer.offer_product_id
),

extracted_offers as (
    select
        offer.offer_id,
        offer_subcategoryid,
        LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.author"), " ")) as author,
        LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.performer"), " ")) as performer,
        LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.musicType"), " ")) as musictype,
        LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.musicSubtype"), " ")) as musicsubtype,
        LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.stageDirector"), " ")) as stagedirector,
        LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.showType"), " ")) as showtype,
        LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.showSubType"), " ")) as showsubtype,
        LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.speaker"), " ")) as speaker,
        LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.rayon"), " ")) as rayon,
        LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.theater.allocine_movie_id"), " ")) as theater_movie_id,
        LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.theater.allocine_room_id"), " ")) as theater_room_id,
        LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.type"), " ")) as movie_type,
        LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.visa"), " ")) as visa,
        LOWER(TRIM(JSON_EXTRACT_SCALAR(extra_data, "$.releaseDate"), " ")) as releasedate,
        LOWER(TRIM(JSON_EXTRACT(extra_data, "$.genres"), " ")) as genres,
        LOWER(TRIM(JSON_EXTRACT(extra_data, "$.companies"), " ")) as companies,
        LOWER(TRIM(JSON_EXTRACT(extra_data, "$.countries"), " ")) as countries,
        LOWER(TRIM(JSON_EXTRACT(extra_data, "$.cast"), " ")) as casting,
        LOWER(TRIM(TRIM(JSON_EXTRACT(extra_data, "$.isbn"), " "), '"')) as isbn,
        LOWER(TRIM(TRIM(JSON_EXTRACT(extra_data, "$.ean"), " "), '"')) as ean,
        LOWER(TRIM(TRIM(JSON_EXTRACT(extra_data, "$.editeur"), " "), '"')) as book_editor,
        LOWER(TRIM(TRIM(JSON_EXTRACT(extra_data, "$.gtl_id"), " "), '"')) as titelive_gtl_id
    from {{ source('raw', 'applicative_database_offer') }} offer
        left join extra_data as ued on ued.offer_id = offer.offer_id
)

select
    * except (isbn, titelive_gtl_id),
    IF(LENGTH(ean) = 13, COALESCE(ean, isbn), isbn) as isbn,
    case
        when LENGTH(CAST(titelive_gtl_id as string)) = 7 then CONCAT('0', CAST(titelive_gtl_id as string))
        else CAST(titelive_gtl_id as string)
    end as titelive_gtl_id
from extracted_offers
