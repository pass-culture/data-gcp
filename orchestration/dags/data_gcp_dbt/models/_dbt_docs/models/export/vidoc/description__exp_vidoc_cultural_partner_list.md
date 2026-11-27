---
title: Active cultural partner list for vidoc export
description: List of the cultural partners active in the current month, from the `exp_vidoc_cultural_partner_list` model.
---

{% docs description__exp_vidoc_cultural_partner_list %}

The `exp_vidoc_cultural_partner_list` model lists the cultural partners (venues) **active in the current month**: at least one bookable individual or collective offer in the last 365 days.
It is the detail behind the latest month of [exp_vidoc_cultural_partner](description__exp_vidoc_cultural_partner.md) and [exp_vidoc_cultural_partner_by_type](description__exp_vidoc_cultural_partner__by_type.md): same activity rule, same filters, same partner types and labels.

{% enddocs %}

## Table Description

Each row is one active partner (venue) in the current month.

**Grain**: `venue_id`. The table is rebuilt at each run and only holds the current month (`partition_month`); it is not a history.

- **Channel**: `is_active_individual`, `is_active_collective`, `is_active_dual_part`. Counting the rows where `is_active_individual` is true gives `total_active_partners_individual` of the current month in `exp_vidoc_cultural_partner`, and the same for collective and dual part; the row count is `total_active_partners_global`.
- **Domain and labels**: one `is_<type>` flag per partner type of the `get_partner_types` macro (`is_librairies`, `is_cinemas`, `is_spectacle_vivant`, ...). `is_tag_*` flags are labels (art et essai cinemas, LIR bookshops, labelled live shows, labelled museums), subsets of a type. A partner can have several flags (a festival is in `musique_live` and `festival`), or none. Counting the rows where `is_<type>` is true gives the current month of `exp_vidoc_cultural_partner_by_type` for that type. `venue_labels` lists all the Ministry of Culture labels of the venue.
- **Location**: current location of the venue; `partner_region_code = '-1'` for overseas collectivities outside any region and non-located partners, as in `exp_vidoc_cultural_partner`.
- No amounts: partner lists are public, out of scope of the statistical secret; revenue and reimbursements are under the business secret and are not in this table.

{% docs table__exp_vidoc__cultural_partner_list %}{% enddocs %}
