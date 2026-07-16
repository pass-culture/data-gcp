# Table: Address

:construction_worker_tone1: This has been created in 2025, is still under development and will be updated in the future.

This model exports address data for ministry use. It provides a list of addresses of representation venues by unique identifier. It contains standardized address information including:

- Unique address identifiers
- Geographic information (postal code, city, department)
- Geographic coordinates (latitude, longitude)
- Administrative codes (INSEE code, BAN ID)

## Table description

| name                    | data_type | description                                                  |
| ----------------------- | --------- | ------------------------------------------------------------ |
| address_id              |           | The unique identifier for the address.                       |
| address_ban             |           | The Base Adresse Nationale (BAN) identifier for the address. |
| address_insee_code      |           | The INSEE code for the address.                              |
| address_street          |           | The street name of the address.                              |
| address_postal_code     |           | The postal code of the address.                              |
| address_city            |           | The city where the address is located.                       |
| address_latitude        |           | The latitude coordinate of the address.                      |
| address_longitude       |           | The longitude coordinate of the address.                     |
| address_department_code |           | The department code for the address.                         |
