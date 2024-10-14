with
    offer_product_data as (
        select
            offer.offer_id,
            coalesce(product.description, offer.offer_description) as offer_description,
            coalesce(product.product_extra_data, offer.offer_extra_data) as extra_data
        from {{ ref("int_source__offer") }} as offer
        left join
            {{ ref("int_applicative__product") }} as product
            on cast(product.id as string) = offer.offer_product_id
    ),

    extracted_offers as (
        select
            offer.* except (offer_description),
            ued.offer_description,
            lower(trim(json_extract_scalar(extra_data, "$.author"), " ")) as author,
            lower(
                trim(json_extract_scalar(extra_data, "$.performer"), " ")
            ) as performer,
            lower(
                trim(json_extract_scalar(extra_data, "$.musicType"), " ")
            ) as musictype,
            lower(
                trim(json_extract_scalar(extra_data, "$.musicSubType"), " ")
            ) as musicsubtype,
            lower(
                trim(json_extract_scalar(extra_data, "$.stageDirector"), " ")
            ) as stagedirector,
            lower(trim(json_extract_scalar(extra_data, "$.showType"), " ")) as showtype,
            lower(
                trim(json_extract_scalar(extra_data, "$.showSubType"), " ")
            ) as showsubtype,
            lower(trim(json_extract_scalar(extra_data, "$.speaker"), " ")) as speaker,
            lower(trim(json_extract_scalar(extra_data, "$.rayon"), " ")) as rayon,
            lower(
                trim(
                    json_extract_scalar(extra_data, "$.theater.allocine_movie_id"), " "
                )
            ) as theater_movie_id,
            lower(
                trim(json_extract_scalar(extra_data, "$.theater.allocine_room_id"), " ")
            ) as theater_room_id,
            lower(trim(json_extract_scalar(extra_data, "$.type"), " ")) as movie_type,
            lower(trim(json_extract_scalar(extra_data, "$.visa"), " ")) as visa,
            lower(
                trim(json_extract_scalar(extra_data, "$.releaseDate"), " ")
            ) as releasedate,
            lower(trim(json_extract(extra_data, "$.genres"), " ")) as genres,
            lower(trim(json_extract(extra_data, "$.companies"), " ")) as companies,
            lower(trim(json_extract(extra_data, "$.countries"), " ")) as countries,
            lower(trim(json_extract(extra_data, "$.cast"), " ")) as casting,
            lower(trim(trim(json_extract(extra_data, "$.isbn"), " "), '"')) as isbn,
            lower(trim(trim(json_extract(extra_data, "$.ean"), " "), '"')) as ean,
            lower(
                trim(trim(json_extract(extra_data, "$.editeur"), " "), '"')
            ) as book_editor,
            lower(
                trim(trim(json_extract(extra_data, "$.gtl_id"), " "), '"')
            ) as titelive_gtl_id,
            lower(
                trim(trim(json_extract(extra_data, "$.bookFormat"), " "), '"')
            ) as book_format,
            lower(
                trim(trim(json_extract(extra_data, "$.comic_series"), " "), '"')
            ) as comic_series,
        from {{ source("raw", "applicative_database_offer") }} offer
        left join offer_product_data as ued on ued.offer_id = offer.offer_id
    )

select
    * except (isbn, titelive_gtl_id),
    if(length(ean) = 13, coalesce(ean, isbn), isbn) as isbn,
    case
        when length(cast(titelive_gtl_id as string)) = 7
        then concat('0', cast(titelive_gtl_id as string))
        else cast(titelive_gtl_id as string)
    end as titelive_gtl_id
from extracted_offers
