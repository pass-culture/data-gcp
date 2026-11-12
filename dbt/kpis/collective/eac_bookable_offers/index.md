## Bookable collective (EAC) offers created and pre-reserved

### Definition

A **bookable collective offer** is an offer made by a cultural partner for one school, with a date, a price and a number of tickets, that the school can pre-reserve. **Showcase offers** ("offres vitrines", templates without date or price, used to start a discussion with schools) are **excluded**.

- **Created (`total_created_bookable_collective_offers`)**: bookable offers created in the month.
- **Pre-reserved (`total_prereserved_bookable_collective_offers`)**: among them, the offers with a non-cancelled booking (pending, confirmed, used or reimbursed).
- **Partners (`new_bookable_offer_partners`)**: cultural partners (venues) creating their first bookable offer of the scholar year in the month.

Same values as the external reporting KPIs `total_offres_reservables_creees`, `total_offres_reservables_prereservees` and `total_partenaires_offres_reservables`.

An offer belongs to the scholar year of its **action** (event date); an offer created before that scholar year starts is counted in its first month (September). A bookable offer is made for one school and has at most one booking, so it is counted once (5 exceptions out of about 594,000 booked offers).

### Published in

**VIDOC** (ministry dashboard):

[exp_vidoc\_\_offer_collective_bookable](https://pass-culture.github.io/data-gcp/dbt/models/export/vidoc/description__exp_vidoc_offer_collective_bookable/index.md) (cultural partner geography)

### Calculation

```
SELECT
    scholar_year,
    venue_region_code,
    venue_region_name,
    SUM(total_created_bookable_collective_offers) AS created_offers,
    SUM(total_prereserved_bookable_collective_offers) AS prereserved_offers,
    SAFE_DIVIDE(
        SUM(total_prereserved_bookable_collective_offers),
        SUM(total_created_bookable_collective_offers)
    ) AS prereservation_rate,
    SUM(new_bookable_offer_partners) AS partners
FROM `offer_collective_bookable`
WHERE scholar_year = '2025-2026'
GROUP BY scholar_year, venue_region_code, venue_region_name
```

Scholar year 2025-2026 (national): 163,197 bookable offers, 99,019 pre-reserved, 12,385 partners.

### Available dimensions

| Dimension    | Column                                           | Description                                                |
| ------------ | ------------------------------------------------ | ---------------------------------------------------------- |
| Month        | `partition_month`                                | Creation month of the offer, clamped into its scholar year |
| Scholar year | `scholar_year`                                   | Scholar year of the action (September to August)           |
| Department   | `venue_department_code`, `venue_department_name` | Department of the cultural partner                         |
| Region       | `venue_region_code`, `venue_region_name`         | Region of the cultural partner                             |

### Temporal tracking

**Monthly**, within the scholar year, from January 2022 to the last complete month. All columns are flows: sum the months (the sum of `new_bookable_offer_partners` gives distinct partners since September).

### Data protection

Offer and partner counts are not counts of people nor revenue: no perturbation and no statistical secret (external reporting publishes them unmasked at region and academy level, and not at department level).

### Notes

Interest of this indicator for the observatoire is still to be decided; it is exported so that vidoc covers it.
