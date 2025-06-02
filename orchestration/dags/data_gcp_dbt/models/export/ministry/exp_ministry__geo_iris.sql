select
    iris_internal_id,
    iris_code,
    iris_label,
    city_code,
    city_label,
    district_code,
    sub_district_code,
    department_code,
    department_name,
    st_astext(iris_centroid) as iris_centroid,
    st_astext(iris_shape) as iris_shape
from {{ ref("int_seed__geo_iris") }}
