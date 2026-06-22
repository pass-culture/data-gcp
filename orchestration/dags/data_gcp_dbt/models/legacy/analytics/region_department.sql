-- TODO deprecated
select
    num_dep,
    dep_name,
    region_code,
    region_name,
    timezone,
    academy_name,
    case
        when num_dep in ("971", "972", "973", "974", "976")
        then "DROM"
        when num_dep in ("975", "977", "978", "986", "987", "988")
        then "COM"
        else "France métropolitaine"
    end as territory_type
from {{ source("seed", "region_department") }}
union all
select
    "-1" as num_dep,
    "non localisé" as dep_name,
    -1 as region_code,
    "non localisé" as region_name,
    cast(null as string) as timezone,
    "non localisé" as academy_name,
    "non localisé" as territory_type
