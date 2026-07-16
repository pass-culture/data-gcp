# Table: Offerer Representation Address

:construction_worker_tone1: This has been created in 2025, is still under development and will be updated in the future. Once the migration between old and new address concepts will be completed, this table will be removed as we will link directly offer_id to address_id.

This model exports to the ministry the offerer representation address data mapping between offerer and new address concept.

It contains information about the addresses where offerers are represented, including:

- Address identifiers (offerer address ID, address ID)
- Address label
- Associated offerer

## Table description

| name                  | data_type | description                                                                                      |
| --------------------- | --------- | ------------------------------------------------------------------------------------------------ |
| offerer_address_id    |           | The unique identifier for the mapping between an offerer and an address where he created offers. |
| offerer_address_label |           | The label for the address.                                                                       |
| address_id            |           | The unique identifier for the address.                                                           |
| offerer_id            |           | Unique identifier of the offerer.                                                                |
