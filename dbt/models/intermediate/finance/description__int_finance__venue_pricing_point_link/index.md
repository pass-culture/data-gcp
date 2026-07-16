The `int_finance__venue_pricing_point_link` table links venues to their pricing points.

## Business Context

The **pricing point** determines how reimbursement amounts are calculated. The reimbursement rate can depend on the pricing point's cumulative revenue (tiered rates).

### Pricing Point Rules

- A venue **with SIRET** is automatically its own pricing point (no action required)
- A venue **without SIRET** must select a pricing point among its structure's venues that have a SIRET

### Temporal Aspect

This link is temporal (timespan): a venue can change its pricing point over time. The `pricing_point_link_beginning_date` and `pricing_point_link_ending_date` columns define the validity period.

### Important Note

Changing a venue's pricing point could reset its cumulative revenue, potentially allowing higher reimbursement rates. Such changes require accounting team validation if the current revenue exceeds 20,000€.

## Table description

| name                              | data_type | description                                                      |
| --------------------------------- | --------- | ---------------------------------------------------------------- |
| id                                |           | Unique identifier for the venue-pricing point link.              |
| venue_id                          |           | Unique identifier for the venue.                                 |
| pricing_point_id                  |           | Identifier of the pricing point (venue with SIRET).              |
| pricing_point_link_beginning_date |           | Start date of the venue-pricing point link.                      |
| pricing_point_link_ending_date    |           | End date of the venue-pricing point link (null if still active). |
