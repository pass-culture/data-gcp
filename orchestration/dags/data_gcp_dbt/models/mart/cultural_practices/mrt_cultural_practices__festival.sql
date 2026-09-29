{{ config(tags=["monthly"]) }}

select
    festival.respondent_id,
    festival.attended_festival_last_12m,
    festival.attended_theatre_dance_street_last_12m,
    festival.attended_classical_opera_jazz_last_12m,
    festival.attended_world_traditional_last_12m,
    festival.attended_rock_varieties_last_12m,
    festival.attended_cinema_last_12m,
    festival.attended_photography_last_12m,
    festival.attended_other_last_12m,
    festival.location_local,
    festival.location_paris,
    festival.location_other_region,
    festival.location_abroad_europe,
    festival.location_abroad_outside_europe,
    festival.timing_holidays,
    festival.timing_weekend,
    festival.with_alone,
    festival.with_partner,
    festival.with_children,
    festival.with_grandchildren,
    festival.with_relatives,
    festival.with_friends,
    festival.with_organized_group,
    festival.with_no_general_rule,
    festival.festival_would_miss_code,
    festival.festival_would_miss,
    respondent.gender,
    respondent.age_group,
    respondent.income,
    respondent.household_type
from {{ ref("int_cultural_practices__festival") }} as festival
inner join
    {{ ref("int_cultural_practices__respondent") }} as respondent
    on festival.respondent_id = respondent.respondent_id
