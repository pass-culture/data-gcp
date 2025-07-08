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
            max(case when stock_rk = 1 then stock_price end) as last_stock_price,
            min(stock_creation_date) as first_stock_creation_date
        from {{ ref("int_applicative__stock") }}
        group by offer_id
    ),

    total_favorites as (
        select offerid, count(*) as total_favorites
        from {{ source("raw", "applicative_database_favorite") }}
        group by offerid
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
        when (
            (offer_validation = "APPROVED" AND ( scheduled_offer_bookability_date <= current_date))
            OR (scheduled_offer_bookability_date IS NULL AND offer_publication_date ≤ current_date)
		)
        then true
        else false
    end as offer_is_bookable,
    stocks_grouped_by_offers.total_available_stock,
    stocks_grouped_by_offers.total_bookings,
    stocks_grouped_by_offers.total_individual_bookings,
    stocks_grouped_by_offers.total_cancelled_individual_bookings,
    stocks_grouped_by_offers.total_non_cancelled_individual_bookings,
    stocks_grouped_by_offers.total_used_individual_bookings,
    stocks_grouped_by_offers.total_individual_theoretic_revenue,
    stocks_grouped_by_offers.total_individual_real_revenue,
    stocks_grouped_by_offers.total_individual_current_year_real_revenue,
    stocks_grouped_by_offers.first_individual_booking_date,
    stocks_grouped_by_offers.last_individual_booking_date,
    stocks_grouped_by_offers.total_stock_quantity,
    stocks_grouped_by_offers.total_first_bookings,
    stocks_grouped_by_offers.last_stock_price,
    stocks_grouped_by_offers.first_stock_creation_date,
    total_favorites.total_favorites,
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
            or stocks_grouped_by_offers.last_stock_price = 0
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
    o.offer_publication_date,
    case
        when o.offer_publication_date > current_date then true else false
    end as is_future_scheduled,
    case
        when
            o.scheduled_offer_bookability_date > current_date
            and o.offer_publication_date <= current_date
        then true
        else false
    end as is_coming_soon,
    ho.total_headlines,
    case
        when
            is_headlined
            and (
                stocks_grouped_by_offers.is_bookable
                and o.offer_is_active
                and o.offer_validation = "APPROVED"
            )
        then true
        else false
    end as is_headlined,
    first_headline_date,
    last_headline_date,
    o.offer_finalization_date,
    o.scheduled_offer_bookability_date
from {{ ref("int_applicative__extract_offer") }} as o
left join {{ ref("int_applicative__offer_item_id") }} as ii on ii.offer_id = o.offer_id
left join stocks_grouped_by_offers on stocks_grouped_by_offers.offer_id = o.offer_id
left join total_favorites on total_favorites.offerid = o.offer_id
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
left join {{ ref("int_applicative__headline_offer") }} as ho on ho.offer_id = o.offer_id
where
    o.offer_subcategoryid not in ("ACTIVATION_THING", "ACTIVATION_EVENT")
    and (o.booking_email <> "jeux-concours@passculture.app" or o.booking_email is null)
