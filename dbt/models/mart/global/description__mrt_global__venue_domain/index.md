# Table: Venue Domain

The `mrt_mapping__venue_domain` table is designed to store the list of the educational domains associated to venues.

Venues can be related to several educational domains, reported during subscription. The table is the corresponding table used to find the exhautive educational domain list of each venue.

## Table description

| name                    | data_type | description                                                                                                                      |
| ----------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------- |
| educational_domain_id   |           | Unique id of the educational domain of the collective offer or venue. An offer or a venue can have multiple educational domains. |
| venue_id                |           | Unique identifier for the venue.                                                                                                 |
| educational_domain_name |           | Unique name corresponding to the id of the educational domain of the collective offer or venue.                                  |
