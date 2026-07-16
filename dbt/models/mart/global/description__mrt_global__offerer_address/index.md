The `mrt_global__offerer_address` table maps each offerer with the addresses where they created offers. An offerer can create offers at multiple address in the same time. An offerer can be related to multiple venues. If the venue is permanent, he is linked to a unique address. An offerer can create offers on venues he did not created. The offerer-address-id maps the different addresses where offerers created offers.

## Table description

This table maps every addresses where offerers created venues or offers.

| name                    | data_type | description                                                                                      |
| ----------------------- | --------- | ------------------------------------------------------------------------------------------------ |
| offerer_address_id      |           | The unique identifier for the mapping between an offerer and an address where he created offers. |
| offerer_address_label   |           | The label for the address.                                                                       |
| address_id              |           | The unique identifier for the address.                                                           |
| offerer_id              |           | Unique identifier of the offerer.                                                                |
| address_street          |           | The street name of the address.                                                                  |
| address_postal_code     |           | The postal code of the address.                                                                  |
| address_city            |           | The city where the address is located.                                                           |
| address_department_code |           | The department code for the address.                                                             |
