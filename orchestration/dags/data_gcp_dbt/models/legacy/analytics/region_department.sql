-- TODO deprecated
select num_dep, dep_name, region_code, region_name, timezone, academy_name
from {{ source("seed", "region_department") }}
union all
select
    "-1" as num_dep,
    "non localisé" as dep_name,
    cast(null as float64) as region_code,
    "non localisé" as region_name,
    cast(null as string) as timezone,
    "non localisé" as academy_name
