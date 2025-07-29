-- TODO deprecated
select 
    num_dep,
    dep_name,
    region_code,
    region_name,
    timezone,
    academy_name
from {{ source("seed", "region_department") }}
