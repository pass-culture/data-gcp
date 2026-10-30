**partner_id**: Unique identifier of the partner.

**cultural_partner_partner_id**: Unique identifier of the cultural partner: the identifier of its venue (equal to venue_id).

**partner_creation_date**: Creation date of the cultural partner (offerer or venue).

**partner_was_registered_last_year**: Boolean for analytical purpose. On year between registration of partner (creation of the venue or the offerer) and current date.

**partner_name**: Name of the cultural partner.

**partner_academy_name**: Name of the academy associated with the cultural partner.

**partner_region_name**: Name of the region where the cultural partner is located.

**partner_region_code**: Code of the region where the cultural partner is located.

**partner_department_code**: Code of the department where the cultural partner is located.

**partner_department_name**: Name of the department where the cultural partner is located.

**partner_postal_code**: Postal code of the cultural partner's location.

**partner_type**: Type of the cultural partner, derived from venue type or venue/offerer tags.

**partner_type_origin**: Origin of the partner type, indicating whether it is derived from venue types, venue tags or offerer tags.

**partner_status**: Status of the cultural partner, indicating whether it is a (permanent) venue or an offerer (without permanent venue).

**partner_cultural_sector**: Cultural sector associated with the partner type.

**partner_epci**: name of the EPCI where the cultural partner is located.

**partner_city**: Name of the city where the cultural partner is located, at the arrondissement level for Paris, Lyon and Marseille (e.g. `Paris 1er Arrondissement`).

**partner_epci_code**: Code of the EPCI where the cultural partner is located.

**partner_city_code**: INSEE code of the city where the cultural partner is located, at the arrondissement level for Paris, Lyon and Marseille (e.g. `75101`).

**partner_municipality_code**: INSEE code of the municipality (commune) where the cultural partner is located: the parent municipality for the arrondissements of Paris, Lyon and Marseille (`75056`, `69123`, `13055`), the same as `partner_city_code` everywhere else.

**partner_municipality_label**: Name of the municipality (commune) of `partner_municipality_code` (e.g. `Paris` for every Paris arrondissement).

**cultural_partner_is_in_qpv**: Boolean. Indicates whether the cultural partner's geocoded address is in a QPV (Quartier Prioritaire de la Ville); null if the address is not geocoded.

**partner_qpv_code**: Code of the QPV containing the cultural partner's geocoded address, null otherwise.

**partner_qpv_name**: Name of the QPV containing the cultural partner's geocoded address; `hors QPV` if geocoded outside any QPV; `non localisé` if not geocoded.

**partner_qpv_municipality**: Municipality (commune) label of the QPV containing the cultural partner, null if the partner is not in a QPV.
