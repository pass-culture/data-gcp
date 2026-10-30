**bookable_history_collective_offer_id**: Collective offer bookable on partition_date: approved active offer with a stock still open for booking and not yet booked, or approved template offer.

**bookable_history_collective_partition_date**: Observation day. Partition column: a partition filter is required.

**bookable_history_collective_offer_is_template**: TRUE for template (showcase) collective offers, which have no date or institution and cannot be booked as such; FALSE for actual bookable offers.

**bookable_history_offer_partition_date**: Observation day. Partition column: a partition filter is required (the table holds several terabytes).

**bookable_history_offer_id**: Offer bookable on partition_date (offers of validated offerers only).

**bookable_history_offer_subcategory_id**: Subcategory of the offer on partition_date.

**bookable_history_offer_category_id**: Category of the offer on partition_date, derived from its subcategory.

**bookable_history_venue_partition_date**: Observation day; partition column (daily). Filtering on it is strongly recommended.

**bookable_history_total_individual_bookable_offers**: Number of individual offers of the venue bookable that day (validated offerers only). A value > 0 means the venue is active on individual offers that day.

**bookable_history_total_collective_bookable_offers**: Number of collective offers of the venue bookable that day.
