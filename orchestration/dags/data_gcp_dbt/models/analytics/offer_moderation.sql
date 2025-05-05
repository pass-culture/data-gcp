{% set target_name = var("ENV_SHORT_NAME") %}
{% set target_schema = generate_schema_name("analytics_" ~ target_name) %}

{{ config(pre_hook="{{create_humanize_id_function()}}") }}

with
    offer_humanized_id as (
        select offer_id, offer_humanized_id
        from {{ ref("int_applicative__offer") }}
        where offer_id is not null
    ),

    venue_humanized_id as (
        select venue_id, venue_humanized_id
        from {{ ref("int_applicative__venue") }}
        where venue_id is not null
    ),

    offerer_humanized_id as (
        select
            offerer_id,
            {{ target_schema }}.humanize_id(offerer_id) as offerer_humanized_id
        from {{ ref("int_raw__offerer") }}
        where offerer_id is not null
    ),

    bookings_days as (
        select distinct
            offer.offer_id,
            date(booking_creation_date) as booking_creation_date,
            count(distinct booking_id) over (
                partition by offer.offer_id, date(booking_creation_date)
            ) as cnt_bookings_day,
            if(
                booking_is_cancelled,
                count(distinct booking_id) over (
                    partition by offer.offer_id, date(booking_creation_date)
                ),
                null
            ) as cnt_bookings_cancelled,
            if(
                not booking_is_cancelled,
                count(distinct booking_id) over (
                    partition by offer.offer_id, date(booking_creation_date)
                ),
                null
            ) as cnt_bookings_confirm
        from {{ ref("int_applicative__booking") }} booking
        join {{ ref("int_global__stock") }} stock using (stock_id)
        join
            {{ ref("int_applicative__offer") }} offer on offer.offer_id = stock.offer_id
    ),

    count_bookings as (
        select
            offer_id,
            max(cnt_bookings_day) as max_bookings_in_day,
            min(booking_creation_date) as first_booking_date,
            sum(cnt_bookings_cancelled) as cnt_bookings_cancelled,
            sum(cnt_bookings_confirm) as cnt_bookings_confirm
        from bookings_days
        group by offer_id
    ),

    offer_stock_ids as (
        select
            offer_id,
            string_agg(distinct stock_id, " ; " order by stock_id) as stocks_ids,
            date(min(stock_beginning_date)) as first_stock_beginning_date,
            date(max(stock_booking_limit_date)) as last_booking_limit_date,
            sum(stock_quantity) as offer_stock_quantity,
            sum(total_available_stock) as available_stock_quantity
        from {{ ref("mrt_global__stock") }}
        group by offer_id
    ),

    last_stock as (
        select offer.offer_id, stock.stock_price as last_stock_price
        from {{ ref("int_applicative__offer") }} as offer
        join {{ ref("mrt_global__stock") }} as stock on stock.offer_id = offer.offer_id
        qualify
            row_number() over (
                partition by stock.offer_id
                order by stock.stock_creation_date desc, stock.stock_id desc
            )
            = 1
    ),

    offer_tags as (
        select
            offer_id,
            string_agg(
                tag_name, " ; " order by cast(oc.criterion_id as int) desc
            ) as playlist_tags
        from {{ ref("mrt_global__offer_criterion") }} oc
        group by offer_id
    ),

    offer_status as (
        select distinct
            offer.offer_id,
            case
                when offer.is_active = false
                then "INACTIVE"
                when
                    (
                        offer.offer_validation like "%APPROVED%"
                        and (
                            sum(stock.total_available_stock) over (
                                partition by offer.offer_id
                            )
                        )
                        <= 0
                    )
                then "SOLD_OUT"
                when
                    (
                        offer.offer_validation like "%APPROVED%"
                        and (
                            max(
                                extract(date from stock.stock_booking_limit_date)
                            ) over (partition by offer.offer_id)
                            < current_date()
                        )
                    )
                then "EXPIRED"
                else offer.offer_validation
            end as offer_status
        from {{ ref("int_applicative__offer") }} offer
        left join
            {{ ref("mrt_global__stock") }} stock on offer.offer_id = stock.offer_id
    ),

    offerer_tags as (
        select
            offerer_id,
            string_agg(
                tag_label, " ; " order by cast(offerer_id as int)
            ) as structure_tags
        from {{ ref("int_applicative__offerer_tag") }}
        group by offerer_id
    )

select distinct
    offer.offer_id,
    offer.offer_name,
    offer.offer_subcategory_id,
    subcategories.category_id,
    subcategories.is_physical_deposit as physical_goods,
    if(subcategories.category_id = 'LIVRE', true, false) as is_book,
    date(offer.offer_creation_date) as offer_creation_date,
    offer.offer_external_ticket_office_url,
    if(offer.offer_id_at_providers is null, "manuel", "synchro") as input_type,
    case
        when
            offer.is_active
            and offer.offer_id
            in (select offer_id from {{ ref("int_global__stock") }} where is_bookable)
        then true
        else false
    end as offer_is_bookable,
    offer.offer_is_duo,
    offer.is_active as offer_is_active,
    offer_status.offer_status,
    if(offer_status.offer_status = 'SOLD_OUT', true, false) as is_sold_out,
    venue.offerer_id,
    offerer.offerer_name,
    venue.venue_id,
    venue.venue_name,
    venue.venue_public_name,
    region_dept.region_name,
    venue.venue_department_code,
    venue.venue_postal_code,
    venue.venue_type_label,
    if(
        venue.venue_label in (
            "SMAC - Scène de musiques actuelles",
            "Théâtre lyrique conventionné d'intérêt national",
            "CNCM - Centre national de création musicale",
            "FRAC - Fonds régional d'art contemporain",
            "Scènes conventionnées",
            "Scène nationale",
            "Théâtres nationaux",
            "CAC - Centre d'art contemporain d'intérêt national",
            "CDCN - Centre de développement chorégraphique national",
            "Orchestre national en région",
            "CCN - Centre chorégraphique national",
            "CDN - Centre dramatique national",
            "Opéra national en région",
            "PNC - Pôle national du cirque",
            "CNAREP - Centre national des arts de la rue et de l'espace public"
        ),
        true,
        false
    ) as is_dgca,
    venue.venue_label,
    venue_humanized_id.venue_humanized_id,
    venue.venue_booking_email,
    venue_contact.venue_contact_phone_number,
    if(
        siren_data.activiteprincipaleunitelegale = "84.11Z", true, false
    ) as is_collectivity,
    offer_humanized_id.offer_humanized_id as offer_humanized_id,
    concat(
        'https://backoffice.passculture.team/pro/offer/', offer.offer_id
    ) as passculture_pro_url,
    concat('https://passculture.app/offre/', offer.offer_id) as webapp_url,
    concat(
        "https://backoffice.passculture.team/pro/offerer/", venue.offerer_id
    ) as link_pc_pro,
    count_bookings.first_booking_date as first_booking_date,
    coalesce(count_bookings.max_bookings_in_day, 0) as max_bookings_in_day,
    coalesce(count_bookings.cnt_bookings_cancelled, 0) as cnt_bookings_cancelled,
    coalesce(count_bookings.cnt_bookings_confirm, 0) as cnt_bookings_confirm,
    date_diff(
        count_bookings.first_booking_date, offer.offer_creation_date, day
    ) as diffdays_creation_firstbooking,
    offer_stock_ids.stocks_ids,
    offer_stock_ids.first_stock_beginning_date,
    offer_stock_ids.last_booking_limit_date,
    offer_stock_ids.offer_stock_quantity,
    offer_stock_ids.available_stock_quantity,
    safe_divide(
        (
            offer_stock_ids.offer_stock_quantity
            - offer_stock_ids.available_stock_quantity
        ),
        offer_stock_ids.offer_stock_quantity
    ) as fill_rate,
    last_stock.last_stock_price,
    offer_tags.playlist_tags,
    offerer_tags.structure_tags

from {{ ref("int_applicative__offer") }} offer
left join {{ ref("int_global__venue") }} venue on venue.venue_id = offer.venue_id
left join venue_humanized_id on venue_humanized_id.venue_id = venue.venue_id
left join
    {{ source("seed", "region_department") }} region_dept
    on region_dept.num_dep = venue.venue_department_code
left join {{ ref("int_raw__offerer") }} offerer on offerer.offerer_id = venue.offerer_id
left join offerer_humanized_id on offerer_humanized_id.offerer_id = offerer.offerer_id
left join {{ ref("siren_data") }} siren_data on siren_data.siren = offerer.offerer_siren
left join offerer_tags on offerer_tags.offerer_id = offerer.offerer_id
left join
    {{ source("raw", "applicative_database_venue_contact") }} venue_contact
    on venue_contact.venue_id = venue.venue_id
left join
    offer_humanized_id as offer_humanized_id
    on offer_humanized_id.offer_id = offer.offer_id
left join
    {{ source("raw", "subcategories") }} subcategories
    on subcategories.id = offer.offer_subcategory_id
left join count_bookings on count_bookings.offer_id = offer.offer_id
left join offer_stock_ids on offer_stock_ids.offer_id = offer.offer_id
left join last_stock on last_stock.offer_id = offer.offer_id
left join offer_status on offer_status.offer_id = offer.offer_id
left join offer_tags on offer_tags.offer_id = offer.offer_id
