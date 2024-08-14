{% set target_name = var('ENV_SHORT_NAME') %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}

with stocks_grouped_by_offers as (
    select
        offer_id,
        SUM(total_available_stock) as total_available_stock,
        MAX(is_bookable) as is_bookable,
        SUM(total_bookings) as total_bookings,
        SUM(total_individual_bookings) as total_individual_bookings,
        SUM(total_cancelled_individual_bookings) as total_cancelled_individual_bookings,
        SUM(total_non_cancelled_individual_bookings) as total_non_cancelled_individual_bookings,
        SUM(total_used_individual_bookings) as total_used_individual_bookings,
        SUM(total_individual_theoretic_revenue) as total_individual_theoretic_revenue,
        SUM(total_individual_real_revenue) as total_individual_real_revenue,
        SUM(total_individual_current_year_real_revenue) as total_individual_current_year_real_revenue,
        MIN(first_individual_booking_date) as first_individual_booking_date,
        MAX(last_individual_booking_date) as last_individual_booking_date,
        SUM(stock_quantity) as total_stock_quantity,
        SUM(total_first_bookings) as total_first_bookings,
        MAX(case when stock_rk = 1 then stock_price else NULL end) as last_stock_price,
        MIN(stock_creation_date) as first_stock_creation_date
    from {{ ref("int_applicative__stock") }}
    group by offer_id
),

total_favorites as (
    select
        offerid,
        COUNT(*) as total_favorites
    from {{ source("raw", "applicative_database_favorite") }}
    group by offerid
)

select
    o.offer_id,
    o.offer_id_at_providers,
    (o.offer_id_at_providers is not NULL) as is_synchronised,
    o.offer_modified_at_last_provider_date,
    DATE(o.offer_creation_date) as offer_creation_date,
    o.offer_creation_date as offer_created_at,
    o.offer_date_updated,
    o.offer_product_id,
    {{ target_schema }}.humanize_id(o.offer_product_id) as offer_product_humanized_id,
    o.venue_id,
    o.offer_last_provider_id,
    o.booking_email,
    o.offer_is_active as is_active,
    o.offer_name,
    o.offer_description,
    o.offer_url,
    CONCAT("https://passculture.pro/offre/individuelle/", o.offer_id, "/informations") as passculture_pro_url,
    CONCAT("https://passculture.app/offre/", o.offer_id) as webapp_url,
    o.offer_duration_minutes,
    o.offer_is_national as is_national,
    o.offer_extra_data,
    o.offer_is_duo,
    o.offer_fields_updated,
    o.offer_withdrawal_details,
    o.offer_audio_disability_compliant,
    o.offer_mental_disability_compliant,
    o.offer_motor_disability_compliant,
    o.offer_visual_disability_compliant,
    o.offer_external_ticket_office_url,
    o.offer_validation,
    o.offer_last_validation_type,
    o.offer_subcategoryid as offer_subcategory_id,
    o.offer_withdrawal_delay,
    o.booking_contact,
    o.author,
    o.performer,
    o.stagedirector as stage_director,
    o.theater_movie_id,
    o.theater_room_id,
    o.speaker,
    o.movie_type,
    o.visa,
    o.releasedate as release_date,
    o.genres,
    o.companies,
    o.countries,
    o.casting,
    o.isbn,
    o.titelive_gtl_id,
    o.offerer_address_id,
    case when (
            so.is_bookable
            and o.offer_is_active
            and o.offer_validation = "APPROVED"
        ) then TRUE
        else FALSE
    end as offer_is_bookable,
    so.total_available_stock,
    so.total_bookings,
    so.total_individual_bookings,
    so.total_cancelled_individual_bookings,
    so.total_non_cancelled_individual_bookings,
    so.total_used_individual_bookings,
    so.total_individual_theoretic_revenue,
    so.total_individual_real_revenue,
    so.total_individual_current_year_real_revenue,
    so.first_individual_booking_date,
    so.last_individual_booking_date,
    so.total_stock_quantity,
    so.total_first_bookings,
    so.last_stock_price,
    so.first_stock_creation_date,
    tf.total_favorites,
    subcategories.is_physical_deposit as physical_goods,
    subcategories.is_digital_deposit as digital_goods,
    subcategories.is_event as event,
    subcategories.category_id as offer_category_id,
    isbn_rayon_editor.rayon,
    isbn_rayon_editor.book_editor,
    ii.item_id,
    m.mediation_humanized_id,
    {{ target_schema }}.humanize_id(o.offer_id) as offer_humanized_id,
    (
        o.offer_subcategoryid not in ("JEU_EN_LIGNE", "JEU_SUPPORT_PHYSIQUE", "ABO_JEU_VIDEO", "ABO_LUDOTHEQUE")
        and (
            o.offer_url is NULL -- not numerical
            or so.last_stock_price = 0
            or subcategories.id = "LIVRE_NUMERIQUE"
            or subcategories.id = "ABO_LIVRE_NUMERIQUE"
            or subcategories.id = "TELECHARGEMENT_LIVRE_AUDIO"
            or subcategories.category_id = "MEDIA"
        )
    ) as offer_is_underage_selectable,
    case
        when subcategories.category_id in ("MUSIQUE_LIVE", "MUSIQUE_ENREGISTREE") then "MUSIC"
        when subcategories.category_id = "SPECTACLE" then "SHOW"
        when subcategories.category_id = "CINEMA" then "MOVIE"
        when subcategories.category_id = "LIVRE" then "BOOK"
    end as offer_type_domain,
    case
        when subcategories.category_id <> "MUSIQUE_LIVE" and o.showtype is not NULL then o.showtype
        when subcategories.category_id = "MUSIQUE_LIVE" then o.musictype
        when subcategories.category_id <> "SPECTACLE" and o.musictype is not NULL then o.musictype
    end as type,
    case
        when subcategories.category_id <> "MUSIQUE_LIVE" and o.showsubtype is not NULL then o.showsubtype
        when subcategories.category_id = "MUSIQUE_LIVE" then o.musicsubtype
        when subcategories.category_id <> "SPECTACLE" and o.musicsubtype is not NULL then o.musicsubtype
    end as subtype,
    future_offer.offer_publication_date,
    case when o.offer_is_active is FALSE and future_offer.offer_publication_date >= CURRENT_DATE then TRUE else FALSE end as is_future_scheduled
from {{ ref("int_applicative__extract_offer") }} as o
    left join {{ ref("offer_item_ids") }} as ii on ii.offer_id = o.offer_id
    left join stocks_grouped_by_offers as so on so.offer_id = o.offer_id
    left join total_favorites as tf on tf.offerid = o.offer_id
    left join {{ source("clean","subcategories") }} as subcategories on o.offer_subcategoryid = subcategories.id
    -- voir pour supprimer les dep à int_applicative__extract_offer et supprimer offer_extracted_data
    left join {{ ref("isbn_rayon_editor") }} as isbn_rayon_editor on o.isbn = isbn_rayon_editor.isbn
    left join
        {{ ref("int_applicative__mediation") }}
            as m
        on o.offer_id = m.offer_id
            and m.is_active
            and m.mediation_rown = 1
    left join {{ source('raw','applicative_database_future_offer') }} as future_offer on future_offer.offer_id = o.offer_id
where o.offer_subcategoryid not in ("ACTIVATION_THING", "ACTIVATION_EVENT")
    and (
        booking_email <> "jeux-concours@passculture.app"
        or booking_email is NULL
    )
