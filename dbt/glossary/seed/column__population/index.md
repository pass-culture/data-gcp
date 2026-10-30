# Population Fields Documentation

**population_snapshot_month**: Month of the population snapshot, as the first day of the month. Population estimates cover whole years up to the last valid INSEE year (INSEE_DATA_LAST_VALID_YEAR), so months after the current one can be present: they carry an estimated population and no observed facts (such as user counts).

**population_decimal_age**: The estimated age of the population in decimal format for more granularity (month over month).

**population_department_code**: The official department code in France.

**population_department_name**: The official name of the department.

**population_academy_name**: The name of the educational academy associated with the department.

**population_region_name**: The name of the region in which the department is located.

**population_territory_type**: The type of territory classification for the department, which can include categories such as metropolitan, overseas, or special territories.

**population_birth_month**: The estimated birth month of individuals based on observed birth trends.

**population_age_decimal_set**: Boolean. TRUE when population_decimal_age is one of the half-year milestones 15, 15.5, 16, 16.5, 17, 17.5, 18, 18.5 or 19; FALSE for all other rows.

**population_age_bracket**: Categorization of the population by age groups:

- `15_17` for ages 15-17
- `18_19` for ages 18-19
- `20_25` for ages 20-25
