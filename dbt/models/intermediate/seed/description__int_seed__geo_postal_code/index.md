# Table: Geographic Postal Code

This model contains approximate centroid coordinates for each postal code in order to be used internally for geocoding in cases we don't have a more precise geocoding based on a street address.

Table is generated from the INSEE Code dataset [2025_insee_code](https://www.data.gouv.fr/fr/datasets/base-officielle-des-codes-postaux/) which gives the centroid of each INSEE code.

## Table description

| name                             | data_type | description                                            |
| -------------------------------- | --------- | ------------------------------------------------------ |
| postal_code                      | STRING    | The postal code of the location.                       |
| postal_approx_centroid_latitude  | FLOAT64   | The approximate centroid latitude of the postal code.  |
| postal_approx_centroid_longitude | FLOAT64   | The approximate centroid longitude of the postal code. |
