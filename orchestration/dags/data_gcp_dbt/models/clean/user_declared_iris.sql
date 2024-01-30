SELECT 
  user_id,
  iris_france.iriscode,
  iris_france.department
FROM {{ source('analytics', 'user_locations') }} AS user_locations
LEFT JOIN {{ source('clean', 'iris_france') }} AS iris_france on user_locations.iris_id = iris_france.id