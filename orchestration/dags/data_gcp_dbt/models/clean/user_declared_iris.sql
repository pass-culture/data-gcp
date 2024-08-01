select
    user_id,
    iris_france.iriscode,
    iris_france.department
from {{ source('analytics', 'user_locations') }} as user_locations
    left join {{ source('clean', 'iris_france') }} as iris_france on user_locations.iris_id = iris_france.id
