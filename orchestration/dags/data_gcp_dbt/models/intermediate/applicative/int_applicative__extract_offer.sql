with
    offer_product_data as (
        select
            offer.offer_id,
            offer.offer_modified_at_last_provider_date,
            offer.offer_creation_date,
            offer.offer_product_id,
            offer.venue_id,
            offer.booking_email,
            offer.offer_is_active,
            offer.offer_name,
            offer.offer_is_duo,
            offer.offer_validation,
            offer.offer_subcategoryid,
            offer.offer_last_validation_type,
            offer.dbt_scd_id,
            offer.dbt_updated_at,
            offer.dbt_valid_from,
            offer.dbt_valid_to,
            offer.offer_id_at_providers,
            offer.offer_last_provider_id,
            offer.offer_url,
            offer.offer_duration_minutes,
            offer.offer_is_national,
            offer.offer_fields_updated,
            offer.offer_withdrawal_details,
            offer.offer_audio_disability_compliant,
            offer.offer_mental_disability_compliant,
            offer.offer_motor_disability_compliant,
            offer.offer_visual_disability_compliant,
            offer.offer_external_ticket_office_url,
            offer.offer_withdrawal_type,
            offer.offer_withdrawal_delay,
            offer.booking_contact,
            offer.offerer_address_id,
            offer.offer_updated_date,
            coalesce(product.description, offer.offer_description) as offer_description,
            coalesce(product.product_extra_data, offer.offer_extra_data) as extra_data,
            coalesce(product.ean, offer.offer_ean) as offer_ean,
            offer.offer_finalization_date,
            offer.offer_publication_date,
            offer.scheduled_offer_bookability_date
        from {{ ref("int_raw__offer") }} as offer
        left join
            {{ ref("int_applicative__product") }} as product
            on cast(product.id as string) = offer.offer_product_id
    )

select
    offer.offer_id,
    offer.offer_modified_at_last_provider_date,
    offer.offer_creation_date,
    offer.offer_product_id,
    offer.venue_id,
    offer.booking_email,
    offer.offer_is_active,
    offer.offer_name,
    offer.offer_is_duo,
    offer.offer_validation,
    offer.offer_subcategoryid,
    offer.offer_last_validation_type,
    offer.dbt_scd_id,
    offer.dbt_updated_at,
    offer.dbt_valid_from,
    offer.dbt_valid_to,
    offer.offer_id_at_providers,
    offer.offer_last_provider_id,
    offer.offer_description,
    offer.offer_url,
    offer.offer_duration_minutes,
    offer.offer_is_national,
    offer.extra_data as offer_extra_data,
    offer.offer_ean,
    offer.offer_fields_updated,
    offer.offer_withdrawal_details,
    offer.offer_audio_disability_compliant,
    offer.offer_mental_disability_compliant,
    offer.offer_motor_disability_compliant,
    offer.offer_visual_disability_compliant,
    offer.offer_external_ticket_office_url,
    offer.offer_withdrawal_type,
    offer.offer_withdrawal_delay,
    offer.booking_contact,
    offer.offerer_address_id,
    offer.offer_updated_date,
    offer.offer_finalization_date,
    offer.offer_publication_date,
    offer.scheduled_offer_bookability_date,
    lower(trim(json_extract_scalar(offer.extra_data, "$.author"), " ")) as author,
    lower(trim(json_extract_scalar(offer.extra_data, "$.performer"), " ")) as performer,
    lower(trim(json_extract_scalar(offer.extra_data, "$.musicType"), " ")) as musictype,
    lower(
        trim(json_extract_scalar(offer.extra_data, "$.musicSubType"), " ")
    ) as musicsubtype,
    lower(
        trim(json_extract_scalar(offer.extra_data, "$.stageDirector"), " ")
    ) as stagedirector,
    lower(trim(json_extract_scalar(offer.extra_data, "$.showType"), " ")) as showtype,
    lower(
        trim(json_extract_scalar(offer.extra_data, "$.showSubType"), " ")
    ) as showsubtype,
    lower(trim(json_extract_scalar(offer.extra_data, "$.speaker"), " ")) as speaker,
    lower(trim(json_extract_scalar(offer.extra_data, "$.rayon"), " ")) as rayon,
    lower(
        trim(json_extract_scalar(offer.extra_data, "$.theater.allocine_movie_id"), " ")
    ) as theater_movie_id,
    lower(
        trim(json_extract_scalar(offer.extra_data, "$.theater.allocine_room_id"), " ")
    ) as theater_room_id,
    lower(trim(json_extract_scalar(offer.extra_data, "$.type"), " ")) as movie_type,
    lower(trim(json_extract_scalar(offer.extra_data, "$.visa"), " ")) as visa,
    lower(
        trim(json_extract_scalar(offer.extra_data, "$.releaseDate"), " ")
    ) as releasedate,
    lower(trim(json_extract(offer.extra_data, "$.genres"), " ")) as genres,
    lower(trim(json_extract(offer.extra_data, "$.companies"), " ")) as companies,
    lower(trim(json_extract(offer.extra_data, "$.countries"), " ")) as countries,
    lower(trim(json_extract(offer.extra_data, "$.cast"), " ")) as casting,
    lower(trim(trim(json_extract(offer.extra_data, "$.isbn"), " "), '"')) as isbn,
    lower(
        trim(trim(json_extract(offer.extra_data, "$.editeur"), " "), '"')
    ) as book_editor,
    lower(
        trim(trim(json_extract(offer.extra_data, "$.gtl_id"), " "), '"')
    ) as titelive_gtl_id,
    lower(
        trim(trim(json_extract(offer.extra_data, "$.bookFormat"), " "), '"')
    ) as book_format,
    lower(
        trim(trim(json_extract(offer.extra_data, "$.comic_series"), " "), '"')
    ) as comic_series
from offer_product_data as offer
