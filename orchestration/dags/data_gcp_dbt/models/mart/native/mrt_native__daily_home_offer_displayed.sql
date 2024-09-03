{{
    config(
        **custom_incremental_config(
        incremental_strategy = 'insert_overwrite',
        partition_by = {'field': 'event_date', 'data_type': 'date'}
    )
) }}

WITH consultations_by_position as (
SELECT 
    native.event_date,
    native.offer_id,
    native.module_id,
    native.displayed_position,
    count(distinct consult.consultation_id) as total_consultations,
FROM {{ ref('int_firebase__native_home_offer_displayed') }} as native 
LEFT JOIN {{ ref('int_firebase__consultation')}} as consult on native.event_date = consult.consultation_date AND native.offer_id = consult.offer_id AND native.module_id = consult.module_id AND consult.origin = "home"
{% if is_incremental() %}
    WHERE date(event_date) = date_sub('{{ ds() }}', INTERVAL 3 day)
    {% else %}
    WHERE date(event_date) >= "2024-06-13"
    {% endif %}
GROUP BY 
    event_date,
    offer_id,
    module_id,
    displayed_position
)

, displays_by_position_bucket AS (
SELECT 
    event_date,
    offer_id,
    module_id,
    COUNT(*) as total_displays,
    COUNT(CASE WHEN displayed_position < 4 THEN 1 ELSE 0 END) as total_position_0_3_diplays,
    COUNT(CASE WHEN displayed_position <= 10 AND displayed_position >= 4 THEN 1 ELSE 0 END) AS total_position_4_10_displays,
    COUNT(CASE WHEN displayed_position <= 20 AND displayed_position > 10 THEN 1 ELSE 0 END) AS total_position_11_20_displays,
    COUNT(CASE WHEN displayed_position <= 30 AND displayed_position > 20 THEN 1 ELSE 0 END) AS total_position_21_30_displays,
    COUNT(CASE WHEN displayed_position <= 40 AND displayed_position > 30 THEN 1 ELSE 0 END) AS total_position_31_40_displays,
    COUNT(CASE WHEN displayed_position <= 50 AND displayed_position > 40 THEN 1 ELSE 0 END) AS total_position_41_50_displays,
FROM {{ ref('int_firebase__native_home_offer_displayed') }} as native 
{% if is_incremental() %}
    WHERE date(event_date) = date_sub('{{ ds() }}', INTERVAL 3 day)
    {% else %}
    WHERE date(event_date) >= "2024-06-13"
    {% endif %}
GROUP BY event_date,
    offer_id,
    module_id
)

, consultations_by_position_bucket AS (
SELECT 
    event_date,
    offer_id,
    module_id,
    SUM(total_consultations) total_consultations,
    SUM(CASE WHEN displayed_position < 4 THEN total_consultations END) AS total_position_0_3_consultations,
    SUM(CASE WHEN displayed_position <= 10 AND displayed_position >= 4 THEN total_consultations END) AS total_position_4_10_consultations,
    SUM(CASE WHEN displayed_position <= 20 AND displayed_position > 10 THEN total_consultations END) AS total_position_11_20_consultations,
    SUM(CASE WHEN displayed_position <= 30 AND displayed_position > 20 THEN total_consultations END) AS total_position_21_30_consultations,
    SUM(CASE WHEN displayed_position <= 40 AND displayed_position > 30 THEN total_consultations END) AS total_position_31_40_consultations,
    SUM(CASE WHEN displayed_position <= 50 AND displayed_position > 40 THEN total_consultations END) AS total_position_41_50_consultations,
FROM consultations_by_position
GROUP BY event_date,
    offer_id,
    module_id
)

SELECT 
    display.event_date,
    display.offer_id,
    display.module_id,
    coalesce(c.title, c.offer_title) AS module_name,
    c.content_type AS module_type,
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
    display.total_displays,
    display.total_position_0_3_diplays,
    display.total_position_4_10_displays,
    display.total_position_11_20_displays,
    display.total_position_21_30_displays,
    display.total_position_31_40_displays,
    display.total_position_41_50_displays,
    consult.total_consultations,
    consult.total_position_0_3_consultations,
    consult.total_position_4_10_consultations,
    consult.total_position_11_20_consultations,
    consult.total_position_21_30_consultations,
    consult.total_position_31_40_consultations,
    consult.total_position_41_50_consultations
FROM displays_by_position_bucket as display
LEFT JOIN consultations_by_position_bucket as consult on display.offer_id = consult.offer_id AND display.event_date = consult.event_date AND display.module_id = consult.module_id
LEFT JOIN {{ ref('mrt_global__offer') }} as offers ON consult.offer_id = offers.offer_id 
left join {{ ref('int_contentful__entry' ) }} as c on c.id = display.module_id