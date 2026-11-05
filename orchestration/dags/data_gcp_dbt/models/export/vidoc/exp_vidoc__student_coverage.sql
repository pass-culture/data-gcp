-- Same source and rules as external_reporting__eac_coverage.
-- ADAGE department aggregates (not individuals): no cell key, no perturbation.
-- Values are snapshots since the start of the scholar year (last ADAGE
-- snapshot of each month); complete months only.
select
    partition_month,
    scholar_year,
    region_code,
    region_name,
    academy_name,
    department_code,
    department_name,
    total_eligible_students,
    total_engaged_students
from {{ ref("metrics_student__coverage") }}
where partition_month < date_trunc(current_date(), month)
