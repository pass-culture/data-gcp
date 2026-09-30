select
    ident18 as respondent_id,
    g331 as attended_theatre_dance_street_last_12m,
    g332 as attended_classical_opera_jazz_last_12m,
    g333 as attended_world_traditional_last_12m,
    g334 as attended_rock_varieties_last_12m,
    g335 as attended_cinema_last_12m,
    g336 as attended_photography_last_12m,
    g337 as attended_other_last_12m,
    g351 as location_local,
    g352 as location_paris,
    g353 as location_other_region,
    g354 as location_abroad_europe,
    g355 as location_abroad_outside_europe,
    g3801 as with_alone,
    g3802 as with_partner,
    g3803 as with_children,
    g3804 as with_grandchildren,
    g3805 as with_relatives,
    g3806 as with_friends,
    g3807 as with_organized_group,
    g3808 as with_no_general_rule,
    g39 as festival_would_miss_code,
    case
        g32 when 1 then 'Oui' when 2 then 'Non' when 3 then 'NSP' when 4 then 'REF'
    end as attended_festival_last_12m,
    case
        g36
        when 1
        then 'Plutôt pendant vos congés ou vacances'
        when 2
        then "Plutôt le reste de l'année"
        when 3
        then 'Pas de règle générale'
        when 4
        then 'NSP'
        when 5
        then 'REF'
    end as timing_holidays,
    case
        g37
        when 1
        then 'Plutôt le week-end'
        when 2
        then 'Plutôt les autres jours de la semaine'
        when 3
        then 'Pas de règle générale'
        when 4
        then 'NSP'
        when 5
        then 'REF'
    end as timing_weekend,
    case
        g39
        when 1
        then 'Oui, beaucoup'
        when 2
        then 'Oui, un peu'
        when 3
        then 'Non, pas tellement'
        when 4
        then 'Non, pas du tout'
        when 5
        then 'NSP'
        when 6
        then 'REF'
    end as festival_would_miss
from {{ ref("int_seed__deps_cultural_practices_2018") }}
