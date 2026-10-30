# Table: Collective Offer Domain

The `mrt_global__collective_offer_domain` table is designed to store the list of the educational domains associated to collective offers.

One row is one (collective offer, educational domain) pair: an offer can carry several domains, so counting offers requires `COUNT(DISTINCT collective_offer_id)` — a plain `COUNT(*)` counts pairs, not offers.

`collective_offer_id` holds ids from two different entities: the model is a `UNION ALL` of `collective_offer_domain` (real offers) and `collective_offer_template_domain` (templates). Template ids are prefixed with `template-`, exactly as in `mrt_global__collective_offer`, because offer ids and template ids share the same id space: a bare template id would collide with a real offer id and attach the template's domains to that offer. A template is a reusable showcase, not a bookable offer, so joins from bookings only reach the real-offer rows; use `collective_offer_is_template` on `mrt_global__collective_offer` (or the `template-` prefix) to pick the population you mean.

The referential is specific to the collective (EAC) side: there is no equivalent governed domain taxonomy for individual offers, so this table cannot answer "by cultural domain" questions about individual bookings.

Collective offers can be related to several educational domain. The table is the corresponding table used to find the exhautive educational domain list of each collective offer.

## Table description

| name                    | data_type | description                                                                                                                      |
| ----------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------- |
| collective_offer_id     |           | Identifier for the collective offer.                                                                                             |
| educational_domain_id   |           | Unique id of the educational domain of the collective offer or venue. An offer or a venue can have multiple educational domains. |
| educational_domain_name |           | Unique name corresponding to the id of the educational domain of the collective offer or venue.                                  |
