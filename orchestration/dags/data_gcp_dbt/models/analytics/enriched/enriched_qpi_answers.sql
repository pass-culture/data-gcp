WITH qpi_v4 as (
  SELECT
    user_id
    , submitted_at
    , subcat.category_id
    , subcategories
FROM {{ ref("qpi_answers_v4") }} uqpi
join {{ source("raw", "subcategories") }} subcat
ON subcat.id=uqpi.subcategories
),

union_all AS (
  SELECT
    user_id
      , submitted_at
      , category_id
      , subcategory_id as subcategories
  FROM {{ source("raw", "qpi_answers_historical") }}
  UNION ALL
  SELECT
    user_id
    , submitted_at
    , category_id
    , subcategories
  FROM qpi_v4
)

SELECT DISTINCT * FROM union_all
QUALIFY RANK() OVER (PARTITION BY user_id ORDER BY submitted_at DESC ) = 1
