{% set target_name = var('ENV_SHORT_NAME') %}
{% set target_schema = generate_schema_name('analytics_' ~ target_name) %}

{{ config(
    pre_hook="{{create_humanize_id_function()}}"
) }}

with offer_humanized_id as (
    select
        offer_id,
        offer_humanized_id
    from {{ ref('int_applicative__offer') }}
    where
        offer_id is not NULL
),

venue_humanized_id as (
    select
        venue_id,
        venue_humanized_id
    from {{ ref('int_applicative__venue') }}
    where
        venue_id is not NULL
),

offerer_humanized_id as (
    select
        offerer_id,
        {{ target_schema }}.humanize_id(offerer_id) as offerer_humanized_id
    from
        {{ ref('offerer') }}
    where
        offerer_id is not NULL
),

bookings_days as (
    select distinct
        offer.offer_id,
        DATE(booking_creation_date) as booking_creation_date,
        COUNT(distinct booking_id) over (partition by offer.offer_id, DATE(booking_creation_date)) as cnt_bookings_day,
        IF(booking_is_cancelled, COUNT(distinct booking_id) over (partition by offer.offer_id, DATE(booking_creation_date)), NULL) as cnt_bookings_cancelled,
        IF(not booking_is_cancelled, COUNT(distinct booking_id) over (partition by offer.offer_id, DATE(booking_creation_date)), NULL) as cnt_bookings_confirm
    from {{ ref('int_applicative__booking') }} booking
        join {{ ref('stock') }} stock using (stock_id)
        join {{ ref('int_applicative__offer') }} offer on offer.offer_id = stock.offer_id
),

count_bookings as (
    select
        offer_id,
        MAX(cnt_bookings_day) as max_bookings_in_day,
        MIN(booking_creation_date) as first_booking_date,
        SUM(cnt_bookings_cancelled) as cnt_bookings_cancelled,
        SUM(cnt_bookings_confirm) as cnt_bookings_confirm
    from
        bookings_days
    group by
        offer_id
),

offer_stock_ids as (
    select
        offer_id,
        STRING_AGG(distinct stock_id, " ; " order by stock_id) as stocks_ids,
        DATE(MIN(stock_beginning_date)) as first_stock_beginning_date,
        DATE(MAX(stock_booking_limit_date)) as last_booking_limit_date,
        SUM(stock_quantity) as offer_stock_quantity,
        SUM(total_available_stock) as available_stock_quantity
    from
        {{ ref('mrt_global__stock') }}
    group by
        offer_id
),

last_stock as (
    select
        offer.offer_id,
        stock.stock_price as last_stock_price
    from
        {{ ref('int_applicative__offer') }} as offer
        join {{ ref('mrt_global__stock') }} as stock on stock.offer_id = offer.offer_id
    qualify ROW_NUMBER() over (partition by stock.offer_id order by stock.stock_creation_date desc, stock.stock_id desc) = 1
),

offer_tags as (
    select
        offerid as offer_id,
        STRING_AGG(name, " ; " order by CAST(criterion.id as INT) desc) as playlist_tags
    from
        {{ ref('offer_criterion') }} offer_criterion
        join {{ ref('criterion') }} criterion on criterion.id = offer_criterion.criterionid
    group by
        offerid
),

offer_status as (
    select distinct
        offer.offer_id,
        case
            when offer.is_active = FALSE then "INACTIVE"
            when (offer.offer_validation like "%APPROVED%" and (SUM(stock.total_available_stock) over (partition by offer.offer_id)) <= 0) then "SOLD_OUT"
            when (offer.offer_validation like "%APPROVED%" and (MAX(EXTRACT(date from stock.stock_booking_limit_date)) over (partition by offer.offer_id) < CURRENT_DATE())) then "EXPIRED"
            else offer.offer_validation
        end as offer_status
    from
        {{ ref('int_applicative__offer') }} offer
        left join {{ ref('mrt_global__stock') }} stock on offer.offer_id = stock.offer_id
),

offerer_tags as (
    select
        offerer_id,
        STRING_AGG(offerer_tag_label, " ; " order by CAST(offerer_id as INT)) as structure_tags
    from
        {{ ref('offerer_tag_mapping') }} offerer_tag_mapping
        left join {{ ref('offerer_tag') }} offerer_tag on offerer_tag_mapping.tag_id = offerer_tag.offerer_tag_id
    group by
        offerer_id
)

select distinct
    offer.offer_id,
    offer.offer_name,
    offer.offer_subcategory_id,
    subcategories.category_id,
    subcategories.is_physical_deposit as physical_goods,
    IF(subcategories.category_id = 'LIVRE', TRUE, FALSE) as is_book,
    DATE(offer.offer_creation_date) as offer_creation_date,
    offer.offer_external_ticket_office_url,
    IF(offer.offer_id_at_providers is NULL, "manuel", "synchro") as input_type,
    case
        when offer.offer_id in (
                select stock.offer_id
                from
                    {{ ref('stock') }} as stock
                    join
                        {{ ref('int_applicative__offer') }}
                            as offer
                        on stock.offer_id = offer.offer_id
                            and offer.is_active
                    join {{ ref('mrt_global__stock') }} as mrt_global__stock on mrt_global__stock.stock_id = stock.stock_id
                where not stock_is_soft_deleted
                    and
                    (
                        (
                            DATE(stock.stock_booking_limit_date) > CURRENT_DATE
                            or stock.stock_booking_limit_date is NULL
                        )
                        and (
                            DATE(stock.stock_beginning_date) > CURRENT_DATE
                            or stock.stock_beginning_date is NULL
                        )
                        and offer.is_active
                        and (
                            mrt_global__stock.total_available_stock > 0
                            or mrt_global__stock.total_available_stock is NULL
                        )
                    )
            ) then TRUE
        else FALSE
    end as offer_is_bookable,
    offer.offer_is_duo,
    offer.is_active AS offer_is_active,
    offer_status.offer_status,
    IF(offer_status.offer_status = 'SOLD_OUT', TRUE, FALSE) as is_sold_out,
    venue.venue_managing_offerer_id as offerer_id,
    offerer.offerer_name,
    venue.venue_id,
    venue.venue_name,
    venue.venue_public_name,
    region_dept.region_name,
    venue.venue_department_code,
    venue.venue_postal_code,
    venue.venue_type_code as venue_type_label,
    IF(venue_label.venue_label in ("SMAC - Scène de musiques actuelles", "Théâtre lyrique conventionné d'intérêt national", "CNCM - Centre national de création musicale", "FRAC - Fonds régional d'art contemporain", "Scènes conventionnées", "Scène nationale", "Théâtres nationaux", "CAC - Centre d'art contemporain d'intérêt national", "CDCN - Centre de développement chorégraphique national", "Orchestre national en région", "CCN - Centre chorégraphique national", "CDN - Centre dramatique national", "Opéra national en région", "PNC - Pôle national du cirque", "CNAREP - Centre national des arts de la rue et de l'espace public"), TRUE, FALSE) as is_dgca,
    venue_label.venue_label as venue_label,
    venue_humanized_id.venue_humanized_id,
    venue.venue_booking_email,
    venue_contact.venue_contact_phone_number,
    IF(siren_data.activiteprincipaleunitelegale = "84.11Z", TRUE, FALSE) as is_collectivity,
    offer_humanized_id.offer_humanized_id as offer_humanized_id,
    CONCAT('https://passculture.pro/offre/individuelle/', offer_humanized_id.offer_humanized_id, '/informations') as passculture_pro_url,
    CONCAT('https://passculture.app/offre/', offer.offer_id) as webapp_url,
    CONCAT("https://passculture.pro/offres?structure=", offerer_humanized_id.offerer_humanized_id) as link_pc_pro,
    count_bookings.first_booking_date as first_booking_date,
    COALESCE(count_bookings.max_bookings_in_day, 0) as max_bookings_in_day,
    COALESCE(count_bookings.cnt_bookings_cancelled, 0) as cnt_bookings_cancelled,
    COALESCE(count_bookings.cnt_bookings_confirm, 0) as cnt_bookings_confirm,
    DATE_DIFF(count_bookings.first_booking_date, offer.offer_creation_date, day) as diffdays_creation_firstbooking,
    offer_stock_ids.stocks_ids,
    offer_stock_ids.first_stock_beginning_date,
    offer_stock_ids.last_booking_limit_date,
    offer_stock_ids.offer_stock_quantity,
    offer_stock_ids.available_stock_quantity,
    SAFE_DIVIDE((offer_stock_ids.offer_stock_quantity - offer_stock_ids.available_stock_quantity), offer_stock_ids.offer_stock_quantity) as fill_rate,
    last_stock.last_stock_price,
    offer_tags.playlist_tags,
    offerer_tags.structure_tags

from
    {{ ref('int_applicative__offer') }} offer
    left join {{ ref('venue') }} venue on venue.venue_id = offer.venue_id
    left join venue_humanized_id on venue_humanized_id.venue_id = venue.venue_id
    left join {{ source('seed', 'region_department') }} region_dept on region_dept.num_dep = venue.venue_department_code
    left join {{ ref('venue_label') }} venue_label on venue_label.venue_label_id = venue.venue_label_id
    left join {{ ref('offerer') }} offerer on offerer.offerer_id = venue.venue_managing_offerer_id
    left join offerer_humanized_id on offerer_humanized_id.offerer_id = offerer.offerer_id
    left join {{ ref('siren_data') }} siren_data on siren_data.siren = offerer.offerer_siren
    left join offerer_tags on offerer_tags.offerer_id = offerer.offerer_id
    left join {{ ref('venue_contact') }} venue_contact on venue_contact.venue_id = venue.venue_id
    left join offer_humanized_id as offer_humanized_id on offer_humanized_id.offer_id = offer.offer_id
    left join {{ source('raw','subcategories') }} subcategories on subcategories.id = offer.offer_subcategory_id
    left join count_bookings on count_bookings.offer_id = offer.offer_id
    left join offer_stock_ids on offer_stock_ids.offer_id = offer.offer_id
    left join last_stock on last_stock.offer_id = offer.offer_id
    left join offer_status on offer_status.offer_id = offer.offer_id
    left join offer_tags on offer_tags.offer_id = offer.offer_id
