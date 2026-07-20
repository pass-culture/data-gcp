select
    partition_month,
    scholar_year,
    region_code,
    region_name,
    academy_name,
    department_code,
    department_name,
    total_eligible_students,
    total_eac_students
from {{ ref("int_kpi__student_coverage") }}
