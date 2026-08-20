{{ config(**custom_table_config()) }}

select
    department_code,
    st_y(st_centroid_agg(iris_centroid)) as department_latitude,
    st_x(st_centroid_agg(iris_centroid)) as department_longitude
from {{ ref("int_seed__geo_iris") }}
where department_code is not null
group by department_code
