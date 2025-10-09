{% set diversification_features = [
    "category",
    "sub_category",
    "format",
    "venue_id",
    "extra_category",
    "venue_type_label",
] %}
{% set delta_diversification_features = [
    "category",
    "sub_category",
    "format",
    "venue_id",
    "extra_category",
] %}

with
    users as (select distinct user_id from {{ ref("int_global__user_beneficiary") }}),

    bookings as (
        select
            bookings.user_id,
            bookings.item_id,
            bookings.offer_id,
            bookings.booking_amount,
            case when bookings.booking_amount = 0 then 1 else 0 end as is_free_offer,
            bookings.booking_created_at as booking_creation_date,
            bookings.booking_id,
            bookings.physical_goods,
            bookings.digital_goods,
            bookings.event,
            bookings.venue_id,
            bookings.venue_type_label

        from {{ ref("mrt_global__booking") }} as bookings
        where booking_status != 'CANCELLED'
    ),

    offer_metadata as (
        select distinct
            offer_id,
            offer_subcategory_id,
            offer_category_id,
            offer_type_label,
            offer_type_domain,
            gtl_type,
            titelive_gtl_id,
            gtl_label_level_1,
            gtl_label_level_2,
            gtl_label_level_3,
            gtl_label_level_4
        from {{ ref("int_applicative__offer_metadata") }}
    ),

    base_diversification as (
        select
            users.user_id,
            bookings.booking_creation_date,
            bookings.booking_id,
            bookings.item_id,
            case
                when subcategories.is_event = true
                then "event"
                when
                    subcategories.online_offline_platform = "ONLINE"
                    and subcategories.is_event = false
                then "digital"
                when
                    subcategories.online_offline_platform
                    in ("OFFLINE", "ONLINE_OR_OFFLINE")
                    and subcategories.is_event = false
                then "physical"
            end as format,
            bookings.offer_id,
            bookings.venue_type_label,
            is_free_offer,
            offer_metadata.offer_category_id as category,
            offer_metadata.offer_subcategory_id as sub_category,
            offer_metadata.offer_type_domain,
            offer_metadata.gtl_type,
            offer_metadata.titelive_gtl_id,
            offer_metadata.gtl_label_level_1,
            offer_metadata.gtl_label_level_2,
            offer_metadata.gtl_label_level_3,
            offer_metadata.gtl_label_level_4,
            -- prendre une venue unique pour les offres digitales
            case
                when bookings.digital_goods = true then "digital_venue" else venue_id
            end as venue_id,
            -- création d'une extra catégorie pour observer la diversification en
            -- genre au sein d'une catégorie(style de musique, genre de film etc...)
            case
                when offer_metadata.offer_type_label is null
                then venue_id
                else offer_metadata.offer_type_label
            end as extra_category,
            -- attribuer un numéro de réservation
            row_number() over (
                partition by users.user_id order by booking_creation_date
            ) as booking_rank
        from users
        inner join bookings on users.user_id = bookings.user_id
        left join offer_metadata on bookings.offer_id = offer_metadata.offer_id
        left join
            {{ source("raw", "subcategories") }} subcategories
            on offer_metadata.offer_subcategory_id = subcategories.id

    ),

    diversification_scores as (
        select
            user_id,
            booking_id,
            item_id,
            booking_rank,
            booking_creation_date,
            is_free_offer,
            category,
            sub_category,
            format,
            extra_category,
            -- Pour attribuer les scores de diversification :
            -- Comparer la date de booking avec la première date de booking sur chaque
            -- feature.
            -- Lorsque ces 2 dates sont les mêmes, attribuer 1 point.
            {% for feature in diversification_features %}
                case
                    when
                        booking_creation_date = min(booking_creation_date) over (
                            partition by user_id, {{ feature }}
                        )
                        and booking_rank != 1
                    then 1
                    else 0
                end as {{ feature }}_diversification
                {% if not loop.last -%}, {%- endif %}
            {% endfor %}
        from base_diversification
    )

select
    user_id,
    item_id,
    booking_id,
    booking_creation_date,
    {% for feature in diversification_features %}
        {{ feature }}_diversification {% if not loop.last -%}, {%- endif %}
    {% endfor %},
    case
        when booking_rank = 1
        then 1  -- 1 point d'office pour le premier booking
        else  -- somme des points de diversification pr les suivants
            {% for feature in delta_diversification_features %}
                {{ feature }}_diversification {% if not loop.last -%} + {%- endif %}
            {% endfor %}
    end as delta_diversification
from diversification_scores
