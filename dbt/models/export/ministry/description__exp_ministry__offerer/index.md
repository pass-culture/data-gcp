# Table: Offerer

This model exports offerer data for ministry use. Offerers are cultural offer providers usually defined by a SIREN number. It contains information about cultural offer providers including:

- Basic information (ID, name, SIREN number)
- Temporal information (creation and validation dates)
- Legal unit information (business activity code and label, legal category code and label)

## Table description

| name                               | data_type | description                                                                                                                   |
| ---------------------------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------- |
| offerer_id                         |           | Unique identifier of the offerer.                                                                                             |
| offerer_name                       |           | Name of the offerer.                                                                                                          |
| offerer_creation_date              |           | Date when the offerer was created. Equals to the registration date of the offerer on the professional portal.                 |
| offerer_validation_date            |           | Date when the offerer was validated (validation_status = ‘VALIDATED’).                                                        |
| offerer_validation_status          |           | Validation status of the offerer.                                                                                             |
| offerer_is_active                  |           | Indicates if the offerer is active. Returns 'true' if offerer is active. Returns 'false' if the offerer has been deactivated. |
| offerer_siren                      |           | SIREN number of the offerer.                                                                                                  |
| legal_unit_business_activity_code  |           | Code of the business activity of the legal unit.                                                                              |
| legal_unit_business_activity_label |           | Label of the business activity of the legal unit.                                                                             |
| legal_unit_legal_category_code     |           | Code of the legal category of the legal unit.                                                                                 |
| legal_unit_legal_category_label    |           | Label of the legal category of the legal unit.                                                                                |
