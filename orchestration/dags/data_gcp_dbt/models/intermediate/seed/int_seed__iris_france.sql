select
    irisf.id, irisf.iriscode, irisf.centroid, irisf.shape, n.region_name, n.department
from {{ source("seed", "iris_france") }} as irisf
left join {{ source("seed", "iris_nesting") }} as n on irisf.iriscode = n.code_iris
