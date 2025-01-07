{% set target_name = var("ENV_SHORT_NAME") %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(pre_hook="{{create_humanize_id_function()}}") }}

with
    stocks_grouped_by_offers as (
        select
            offer_id,
            sum(total_available_stock) as total_available_stock,
            max(is_bookable) as is_bookable,
            sum(total_bookings) as total_bookings,
            sum(total_individual_bookings) as total_individual_bookings,
            sum(
                total_cancelled_individual_bookings
            ) as total_cancelled_individual_bookings,
            sum(
                total_non_cancelled_individual_bookings
            ) as total_non_cancelled_individual_bookings,
            sum(total_used_individual_bookings) as total_used_individual_bookings,
            sum(
                total_individual_theoretic_revenue
            ) as total_individual_theoretic_revenue,
            sum(total_individual_real_revenue) as total_individual_real_revenue,
            sum(
                total_individual_current_year_real_revenue
            ) as total_individual_current_year_real_revenue,
            min(first_individual_booking_date) as first_individual_booking_date,
            max(last_individual_booking_date) as last_individual_booking_date,
            sum(stock_quantity) as total_stock_quantity,
            sum(total_first_bookings) as total_first_bookings,
            max(
                case when stock_rk = 1 then stock_price else null end
            ) as last_stock_price,
            min(stock_creation_date) as first_stock_creation_date
        from {{ ref("int_applicative__stock") }}
        group by offer_id
    ),

    total_favorites as (
        select offerid, count(*) as total_favorites
        from {{ source("raw", "applicative_database_favorite") }}
        group by offerid
    ),

    headlines_grouped_by_offers as (
        select
            offer_id,
            max(headline_offer_end_date) as offer_last_headline_date,
            count(*) as total_offer_headlines,
            count(
                case when headline_offer_end_date is not null then 1 end
            ) as total_headline_offers_not_expired
        from {{ ref("int_applicative__headline_offer") }}
        group by offer_id
    )

select
    o.offer_id,
    o.offer_id_at_providers,
    (o.offer_last_provider_id is not null) as is_synchronised,
    o.offer_modified_at_last_provider_date,
    date(o.offer_creation_date) as offer_creation_date,
    o.offer_creation_date as offer_created_at,
    o.offer_updated_date,
    o.offer_product_id,
    {{ target_schema }}.humanize_id(o.offer_product_id) as offer_product_humanized_id,
    o.venue_id,
    o.offer_last_provider_id,
    o.booking_email,
    o.offer_is_active as is_active,
    o.offer_name,
    o.offer_description,
    o.offer_url,
    concat(
        "https://backoffice.passculture.team/pro/offer/", o.offer_id
    ) as passculture_pro_url,
    concat("https://passculture.app/offre/", o.offer_id) as webapp_url,
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
    offer_ean.isbn,
    offer_ean.ean,
    o.titelive_gtl_id,
    o.offerer_address_id,
    case
        when
            subcategories.category_id in ("MUSIQUE_LIVE", "MUSIQUE_ENREGISTREE")
            and o.musictype != ''
        then o.musictype
        when subcategories.category_id = "SPECTACLE" and o.showtype != ''
        then o.showtype
    end as offer_type_id,
    case
        when
            subcategories.category_id in ("MUSIQUE_LIVE", "MUSIQUE_ENREGISTREE")
            and o.musictype != ''
        then o.musicsubtype
        when subcategories.category_id = "SPECTACLE" and o.showtype != ''
        then o.showsubtype
    end as offer_sub_type_id,
    case
        when (so.is_bookable and o.offer_is_active and o.offer_validation = "APPROVED")
        then true
        else false
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
    subcategories.search_group_name,
    offer_ean.rayon,
    offer_ean.book_editor,
    ii.item_id,
    m.mediation_humanized_id,
    {{ target_schema }}.humanize_id(o.offer_id) as offer_humanized_id,
    (
        o.offer_subcategoryid not in (
            "JEU_EN_LIGNE", "JEU_SUPPORT_PHYSIQUE", "ABO_JEU_VIDEO", "ABO_LUDOTHEQUE"
        )
        and (
            o.offer_url is null  -- not numerical
            or so.last_stock_price = 0
            or subcategories.id = "LIVRE_NUMERIQUE"
            or subcategories.id = "ABO_LIVRE_NUMERIQUE"
            or subcategories.id = "TELECHARGEMENT_LIVRE_AUDIO"
            or subcategories.category_id = "MEDIA"
        )
    ) as offer_is_underage_selectable,
    case
        when subcategories.category_id in ("MUSIQUE_LIVE", "MUSIQUE_ENREGISTREE")
        then "MUSIC"
        when subcategories.category_id = "SPECTACLE"
        then "SHOW"
        when subcategories.category_id = "CINEMA"
        then "MOVIE"
        when subcategories.category_id = "LIVRE"
        then "BOOK"
    end as offer_type_domain,
    case
        when subcategories.category_id <> "MUSIQUE_LIVE" and o.showtype is not null
        then o.showtype
        when subcategories.category_id = "MUSIQUE_LIVE"
        then o.musictype
        when subcategories.category_id <> "SPECTACLE" and o.musictype is not null
        then o.musictype
    end as type,
    case
        when subcategories.category_id <> "MUSIQUE_LIVE" and o.showsubtype is not null
        then o.showsubtype
        when subcategories.category_id = "MUSIQUE_LIVE"
        then o.musicsubtype
        when subcategories.category_id <> "SPECTACLE" and o.musicsubtype is not null
        then o.musicsubtype
    end as subtype,
    future_offer.offer_publication_date,
    case
        when
            o.offer_is_active is false
            and future_offer.offer_publication_date >= current_date
        then true
        else false
    end as is_future_scheduled,
    ho.total_offer_headlines,
    case
        when
            (
                ho.offer_last_headline_date >= current_date
                or ho.total_headline_offers_not_expired > 0
            )
            and (
                so.is_bookable and o.offer_is_active and o.offer_validation = "APPROVED"
            )
        then true
        else false
    end as offer_is_headlined
from {{ ref("int_applicative__extract_offer") }} as o
left join {{ ref("int_applicative__offer_item_id") }} as ii on ii.offer_id = o.offer_id
left join stocks_grouped_by_offers as so on so.offer_id = o.offer_id
left join total_favorites as tf on tf.offerid = o.offer_id
left join
    {{ source("raw", "subcategories") }} as subcategories
    on o.offer_subcategoryid = subcategories.id
left join
    {{ ref("int_applicative__offer_ean") }} as offer_ean
    on o.offer_id = offer_ean.offer_id
left join
    {{ ref("int_applicative__mediation") }} as m
    on o.offer_id = m.offer_id
    and m.is_active
    and m.mediation_rown = 1
left join
    {{ source("raw", "applicative_database_future_offer") }} as future_offer
    on future_offer.offer_id = o.offer_id
where
    o.offer_subcategoryid not in ("ACTIVATION_THING", "ACTIVATION_EVENT")
    and (o.booking_email <> "jeux-concours@passculture.app" or o.booking_email is null)
left join headlines_grouped_by_offers as ho on ho.offer_id = o.offer_id
