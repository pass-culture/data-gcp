select
    ident18 as respondent_id,
    age,
    nhab as household_size,
    case sexe when 1 then 'Homme' when 2 then 'Femme' end as gender,
    case
        critage
        when 1
        then '15-29 ans'
        when 2
        then '30-44 ans'
        when 3
        then '45-59 ans'
        when 4
        then '60-74 ans'
        when 5
        then '75 ans ou plus'
    end as age_group,
    case
        critrevenu
        when 1
        then 'Moins de 800 euros'
        when 2
        then 'De 800 à 999 euros'
        when 3
        then 'De 1000 à 1199 euros'
        when 4
        then 'De 1200 à 1499 euros'
        when 5
        then 'De 1500 à 1999 euros'
        when 6
        then 'De 2000 à 2499 euros'
        when 7
        then 'De 2500 à 2999 euros'
        when 8
        then 'De 3000 à 3999 euros'
        when 9
        then 'De 4000 à 5999 euros'
        when 10
        then '6000 euros ou plus'
        when 11
        then 'Ne sait pas'
        when 12
        then 'Refus'
    end as income,
    case
        typmen
        when 1
        then 'Personne seule'
        when 2
        then 'Famille monoparentale'
        when 3
        then 'Couple sans enfant'
        when 4
        then 'Couple avec enfant'
        when 5
        then 'Ménage complexe'
    end as household_type
from {{ ref("int_seed__deps_cultural_practices_2018") }}
