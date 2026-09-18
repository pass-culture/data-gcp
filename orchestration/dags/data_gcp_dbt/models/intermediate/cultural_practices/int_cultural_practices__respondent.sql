select
    ident18 as respondent_id,
    sexe as gender,
    age,
    critage as age_group,
    critrevenu as income,
    typmen as household_type,
    nhab as household_size
from `passculture-data-prod`.`seed_prod`.`deps_cultural_practices_2018`
