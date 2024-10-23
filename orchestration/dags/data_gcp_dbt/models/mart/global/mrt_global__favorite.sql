select
    f.favorite_id,
    f.favorite_creation_date,
    f.favorite_created_at,
    f.user_id,
    f.offer_id,
    o.offer_name,
    o.offer_category_id,
    o.offer_creation_date,
    o.offer_subcategory_id,
    o.venue_id,
    o.venue_name,
    o.venue_label,
    o.partner_id,
    o.offerer_id,
    o.offerer_name,
    o.venue_type_label,
    o.venue_region_name,
    o.venue_department_code,
    o.venue_department_name,
    o.venue_postal_code,
    o.venue_city,
    o.venue_academy_name,
    o.venue_density_label,
    o.venue_macro_density_label,
    o.venue_density_level,
    u.user_department_code,
    u.user_department_name,
    u.user_postal_code,
    u.user_city,
    u.user_activity,
    u.user_civility,
    u.user_age,
    u.user_is_priority_public,
    u.user_is_unemployed,
    u.user_is_in_education,
    u.user_is_in_qpv,
    u.current_deposit_type,
    u.user_epci,
    u.user_density_label,
    u.city_code,
    u.user_macro_density_label,
    u.user_region_name,
    u.user_academy_name,

from {{ ref("int_applicative__favorite") }} f
left join {{ ref("int_global__offer") }} o on f.offer_id = o.offer_id
left join {{ ref("int_global__user") }} u on u.user_id = f.user_id
