{{
    config(
        **custom_incremental_config(
            incremental_strategy="insert_overwrite",
            partition_by={"field": "event_date", "data_type": "date"},
            on_schema_change="append_new_columns",
            require_partition_filter=true,
        )
    )
}}

with
    consultations_by_position as (
        select
            native.event_date,
            native.offer_id,
            native.module_id,
            native.entry_id,
            native.displayed_position,
            count(distinct consult.consultation_id) as total_consultations
        from {{ ref("int_firebase__native_home_offer_displayed") }} as native
        left join
            {{ ref("int_firebase__native_consultation") }} as consult
            on native.event_date = consult.consultation_date
            and native.offer_id = consult.offer_id
            and native.module_id = consult.module_id
            and consult.origin = "home"
        {% if is_incremental() %}
            where date(event_date) = date_sub('{{ ds() }}', interval 3 day)
        {% else %} where date(event_date) >= "2024-06-13"
        {% endif %}
        group by event_date, offer_id, module_id, entry_id, displayed_position
    ),

    displays_by_position_bucket as (
        select
            event_date,
            offer_id,
            module_id,
            entry_id,
            count(*) as total_displays,
            sum(
                case when displayed_position < 4 then 1 else 0 end
            ) as total_position_0_3_displays,
            sum(
                case
                    when displayed_position <= 10 and displayed_position >= 4
                    then 1
                    else 0
                end
            ) as total_position_4_10_displays,
            sum(
                case
                    when displayed_position <= 20 and displayed_position > 10
                    then 1
                    else 0
                end
            ) as total_position_11_20_displays,
            sum(
                case
                    when displayed_position <= 30 and displayed_position > 20
                    then 1
                    else 0
                end
            ) as total_position_21_30_displays,
            sum(
                case
                    when displayed_position <= 40 and displayed_position > 30
                    then 1
                    else 0
                end
            ) as total_position_31_40_displays,
            sum(
                case
                    when displayed_position <= 50 and displayed_position > 40
                    then 1
                    else 0
                end
            ) as total_position_41_50_displays
        from {{ ref("int_firebase__native_home_offer_displayed") }}
        {% if is_incremental() %}
            where date(event_date) = date_sub('{{ ds() }}', interval 3 day)
        {% else %} where date(event_date) >= "2024-06-13"
        {% endif %}
        group by event_date, offer_id, module_id, entry_id
    ),

    consultations_by_position_bucket as (
        select
            event_date,
            offer_id,
            module_id,
            entry_id,
            sum(total_consultations) as total_consultations,
            sum(
                case when displayed_position < 4 then total_consultations end
            ) as total_position_0_3_consultations,
            sum(
                case
                    when displayed_position <= 10 and displayed_position >= 4
                    then total_consultations
                end
            ) as total_position_4_10_consultations,
            sum(
                case
                    when displayed_position <= 20 and displayed_position > 10
                    then total_consultations
                end
            ) as total_position_11_20_consultations,
            sum(
                case
                    when displayed_position <= 30 and displayed_position > 20
                    then total_consultations
                end
            ) as total_position_21_30_consultations,
            sum(
                case
                    when displayed_position <= 40 and displayed_position > 30
                    then total_consultations
                end
            ) as total_position_31_40_consultations,
            sum(
                case
                    when displayed_position <= 50 and displayed_position > 40
                    then total_consultations
                end
            ) as total_position_41_50_consultations
        from consultations_by_position
        group by event_date, offer_id, module_id, entry_id
    )

select
    display.event_date,
    display.offer_id,
    display.module_id,
    display.entry_id,
    c.content_type as module_type,
    offers.offer_category_id,
    offers.offer_subcategory_id,
    offers.venue_name,
    offers.venue_id,
    offers.venue_density_label,
    offers.venue_macro_density_label,
    offers.partner_id,
    offers.offerer_id,
    offers.offerer_name,
    offers.venue_type_label,
    coalesce(c.title, c.offer_title) as module_name,
    coalesce(display.total_displays, 0) as total_displays,
    coalesce(display.total_position_0_3_displays, 0) as total_position_0_3_displays,
    coalesce(display.total_position_4_10_displays, 0) as total_position_4_10_displays,
    coalesce(display.total_position_11_20_displays, 0) as total_position_11_20_displays,
    coalesce(display.total_position_21_30_displays, 0) as total_position_21_30_displays,
    coalesce(display.total_position_31_40_displays, 0) as total_position_31_40_displays,
    coalesce(display.total_position_41_50_displays, 0) as total_position_41_50_displays,
    coalesce(consult.total_consultations, 0) as total_consultations,
    coalesce(
        consult.total_position_0_3_consultations, 0
    ) as total_position_0_3_consultations,
    coalesce(
        consult.total_position_4_10_consultations, 0
    ) as total_position_4_10_consultations,
    coalesce(
        consult.total_position_11_20_consultations, 0
    ) as total_position_11_20_consultations,
    coalesce(
        consult.total_position_21_30_consultations, 0
    ) as total_position_21_30_consultations,
    coalesce(
        consult.total_position_31_40_consultations, 0
    ) as total_position_31_40_consultations,
    coalesce(
        consult.total_position_41_50_consultations, 0
    ) as total_position_41_50_consultations
from displays_by_position_bucket as display
left join
    consultations_by_position_bucket as consult
    on display.offer_id = consult.offer_id
    and display.event_date = consult.event_date
    and display.module_id = consult.module_id
    and display.entry_id = consult.entry_id
left join {{ ref("mrt_global__offer") }} as offers on consult.offer_id = offers.offer_id
left join {{ ref("int_contentful__entry") }} as c on display.module_id = c.id
