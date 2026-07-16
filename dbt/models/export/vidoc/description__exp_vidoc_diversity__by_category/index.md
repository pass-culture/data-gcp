The `exp_vidoc_diversity__by_category` model aggregates key performance indicators to quantify diversity within beneficiary bookings on the pass Culture app, for each offer categories. It is designed to be exported to ministry for vidoc visualisation.

## Table Description

Each row represents a key indicator calculated for a specific month, geographic aggregation and beneficiary dimensions level.

**Grain**: `deposit_expiration_month`, `region_code`, `department_code`, `is_in_qpv`, `macro_density_label`, `micro_density_label`, `offer_category_id`.

> Rows are **only emitted** for `(cell, category)` pairs where at least one booking exists. Cells with zero bookings for a given category are **absent** from the table.

| name                                | data_type | description                                                                                                                     |
| ----------------------------------- | --------- | ------------------------------------------------------------------------------------------------------------------------------- |
| deposit_expiration_month            |           | The month when the beneficiary's deposit expires.                                                                               |
| region_name                         |           | The official name of the region containing the IRIS area.                                                                       |
| region_code                         |           | The official code of the region (région) containing the IRIS area.                                                              |
| department_name                     |           | The official name of the department containing the IRIS area.                                                                   |
| department_code                     |           | The official code of the department (département) containing the IRIS area.                                                     |
| is_in_qpv                           |           | Boolean. Indicates if the user's registered address is in a priority neighborhood (QPV).                                        |
| macro_density_label                 |           | Macro density label of the user's registered address.                                                                           |
| micro_density_label                 |           | String column.Density label (urban, rural) of the user's registered address.                                                    |
| offer_category_id                   |           | Identifier for the category of the offer. Determined by the cultural partner via a list of pre-set options in a drop down menu. |
| total_category_booked_beneficiaries |           | The number of beneficiaries who made at least one booking in the specific category by the time their credit expired.            |
