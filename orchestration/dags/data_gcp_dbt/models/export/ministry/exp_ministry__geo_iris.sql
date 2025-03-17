{{ config(tags="monthly", labels={"schedule": "monthly"}) }}

select
    iris_internal_id,
    iris_label,
    city_code,
    city_label,
    district_code,
    sub_district_code,
    department_code,
    department_name,
from {{ ref("int_seed__geo_iris") }}
