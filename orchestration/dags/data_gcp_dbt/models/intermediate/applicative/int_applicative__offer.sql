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

    offer_types as (
        select distinct
            upper(domain) as offer_type_domain,
            cast(type as string) as offer_type_id,
            label as offer_type_label
        from {{ source("raw", "offer_types") }}
    ),

    offer_sub_types as (
        select distinct
            upper(domain) as offer_type_domain,
            cast(type as string) as offer_type_id,
            label as offer_type_label,
            safe_cast(safe_cast(sub_type as float64) as string) as offer_sub_type_id,
            sub_label as offer_sub_type_label
        from {{ source("raw", "offer_types") }}
    ),
    
    aggregated_offer as(
        select
            o.offer_id,
            o.offer_id_at_providers,
            (o.offer_last_provider_id is not null) as is_synchronised,
            o.offer_modified_at_last_provider_date,
            date(o.offer_creation_date) as offer_creation_date,
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
            concat(
                "https://passculture.pro/offre/individuelle/", o.offer_id, "/informations"
            ) as passculture_pro_url,
            concat("https://passculture.app/offre/", o.offer_id) as webapp_url,
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
            isbn_rayon_editor.rayon,
            isbn_rayon_editor.book_editor,
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
            future_offer.offer_publication_date
        from {{ ref("int_applicative__extract_offer") }} as o
        left join {{ ref("int_applicative__offer_item_id") }} as ii on ii.offer_id = o.offer_id
        left join stocks_grouped_by_offers as so on so.offer_id = o.offer_id
        left join total_favorites as tf on tf.offerid = o.offer_id
        left join
            {{ source("raw", "subcategories") }} as subcategories
            on o.offer_subcategoryid = subcategories.id
        left join
            {{ ref("int_applicative__isbn_rayon_editor") }} as isbn_rayon_editor
            on o.isbn = isbn_rayon_editor.isbn
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
            and (booking_email <> "jeux-concours@passculture.app" or booking_email is null)
    )

    select
    offer_id,
    offer_id_at_providers,
    is_synchronised,
    offer_modified_at_last_provider_date,
    offer_creation_date,
    offer_created_at,
    offer_date_updated,
    offer_product_id,
    offer_product_humanized_id,
    venue_id,
    offer_last_provider_id,
    booking_email,
    is_active,
    offer_name,
    offer_description,
    offer_url,
    passculture_pro_url,
    webapp_url,
    offer_duration_minutes,
    is_national,
    offer_extra_data,
    offer_is_duo,
    offer_fields_updated,
    offer_withdrawal_details,
    offer_audio_disability_compliant,
    offer_mental_disability_compliant,
    offer_motor_disability_compliant,
    offer_visual_disability_compliant,
    offer_external_ticket_office_url,
    offer_validation,
    offer_last_validation_type,
    offer_subcategory_id,
    offer_withdrawal_delay,
    booking_contact,
    author,
    performer,
    stage_director,
    theater_movie_id,
    theater_room_id,
    speaker,
    movie_type,
    visa,
    release_date,
    genres,
    companies,
    countries,
    casting,
    isbn,
    titelive_gtl_id,
    offerer_address_id,
    o.offer_type_id,
    o.offer_sub_type_id,
    offer_is_bookable,
    total_available_stock,
    total_bookings,
    total_individual_bookings,
    total_cancelled_individual_bookings,
    total_non_cancelled_individual_bookings,
    total_used_individual_bookings,
    total_individual_theoretic_revenue,
    total_individual_real_revenue,
    total_individual_current_year_real_revenue,
    first_individual_booking_date,
    last_individual_booking_date,
    total_stock_quantity,
    total_first_bookings,
    last_stock_price,
    first_stock_creation_date,
    total_favorites,
    physical_goods,
    digital_goods,
    event,
    offer_category_id,
    search_group_name,
    o.rayon,
    book_editor,
    item_id,
    mediation_humanized_id,
    offer_humanized_id,
    offer_is_underage_selectable,
    o.offer_type_domain,
    type,
    subtype,
    offer_publication_date,
    is_future_scheduled,
        case
        when o.offer_type_domain = "MUSIC"
        then offer_types.offer_type_label
        when o.offer_type_domain = "SHOW"
        then offer_types.offer_type_label
        when o.offer_type_domain = "MOVIE"
        then regexp_extract_all(upper(genres), r'[0-9a-zA-Z][^"]+')[safe_offset(0)]
        when o.offer_type_domain = "BOOK"
        then macro_rayons.macro_rayon
    end as offer_type_label,
        case
        when o.offer_type_domain = "MUSIC"
        then
            if(
                offer_types.offer_type_label is null,
                null,
                [offer_types.offer_type_label]
            )
        when o.offer_type_domain = "SHOW"
        then
            if(
                offer_types.offer_type_label is null,
                null,
                [offer_types.offer_type_label]
            )
        when o.offer_type_domain = "MOVIE"
        then regexp_extract_all(upper(genres), r'[0-9a-zA-Z][^"]+')
        when o.offer_type_domain = "BOOK"
        then if(macro_rayons.macro_rayon is null, null, [macro_rayons.macro_rayon])
    end as offer_type_labels,
    case
        when o.offer_type_domain = "MUSIC"
        then offer_sub_types.offer_sub_type_label
        when o.offer_type_domain = "SHOW"
        then offer_sub_types.offer_sub_type_label
        when o.offer_type_domain = "MOVIE"
        then null
        when o.offer_type_domain = "BOOK"
        then o.rayon
    end as offer_sub_type_label
from aggregated_offer o
left join
    offer_types
    on offer_types.offer_type_domain = o.offer_type_domain
    and offer_types.offer_type_id = o.offer_type_id
left join
    offer_sub_types
    on offer_sub_types.offer_type_domain = o.offer_type_domain
    and offer_sub_types.offer_type_id = o.offer_type_id
    and offer_sub_types.offer_sub_type_id = o.offer_sub_type_id
left join {{ source("seed", "macro_rayons") }} on o.rayon = macro_rayons.rayon
