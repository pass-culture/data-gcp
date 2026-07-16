# Table: Offerer Removed

This model exports offerer removed data for ministry use. It contains only removed offerers not longer available for booking in pass Culture apps.

## Table description

| name                      | data_type | description                                                                                                                   |
| ------------------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------- |
| offerer_id                |           | Unique identifier of the offerer.                                                                                             |
| offerer_name              |           | Name of the offerer.                                                                                                          |
| offerer_creation_date     |           | Date when the offerer was created. Equals to the registration date of the offerer on the professional portal.                 |
| offerer_validation_date   |           | Date when the offerer was validated (validation_status = ‘VALIDATED’).                                                        |
| offerer_validation_status |           | Validation status of the offerer.                                                                                             |
| offerer_is_active         |           | Indicates if the offerer is active. Returns 'true' if offerer is active. Returns 'false' if the offerer has been deactivated. |
| offerer_siren             |           | SIREN number of the offerer.                                                                                                  |
