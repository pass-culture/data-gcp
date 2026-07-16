The `mrt_global__educational_deposit` table lists all credits received by educational institutions as part of the collective component of the pass Culture.

Educational institutions partners in the program (`educational_institution_id`) receive an annual budget (`amount`) for each school year (`educational_year_id`). This budget is shared among all the classes in the institution to organize school trips. These budgets are provided to the pass Culture by the various ministries involved in the program (identified through a `ministry` field).

## Table description

| name                              | data_type | description                                                                                                                                                                |
| --------------------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| educational_deposit_id            |           | Id of the educational deposit.                                                                                                                                             |
| institution_id                    |           | Id of the institution.                                                                                                                                                     |
| educational_year_id               |           | Id of the scholar year.                                                                                                                                                    |
| scholar_year                      |           | Scholar year of the deposit.                                                                                                                                               |
| calendar_year                     |           | Calendar year of the deposit.                                                                                                                                              |
| educational_deposit_period        |           | Period associated to the deposit : sept-dec, jan-aug, all_year (sept-aug).                                                                                                 |
| institution_department_code       |           | Department code of the educational institution.                                                                                                                            |
| institution_academy_name          |           | Academy name of the educational institution.                                                                                                                               |
| educational_deposit_amount        |           | Amount received by the educational institution for this scholar year, in euros.                                                                                            |
| is_current_scholar_year           |           | Whether or not the deposit is associated to the current scholar year (scholar year = a year from 1st Sept to 31 Aug, ex : 2024-2025, from 1st Sept 2024 to 31th Aug 2025). |
| is_current_calendar_year          |           | Whether or not the deposit is associated to the current calendar year (calendar year = a year from 1st of Jan to 31th of dec, ex : 2025 from 01/01 to 12/31).              |
| is_current_deposit                |           | Whether or not the deposit is associated to the current period.                                                                                                            |
| educational_deposit_creation_date |           | Upload date of the educational deposit.                                                                                                                                    |
