## Diversity

Diversity of the cultural practices of young people at credit expiration: number of categories booked, categories and distinct venues.

**Data protection**: Counts are perturbed (cell key method); the number of distinct venues is hidden when fewer than 5 beneficiaries are in the row. See [Statistical confidentiality](https://pass-culture.github.io/data-gcp/dbt/references/statistical_confidentiality/index.md).

| KPI                                                                                                                                   | Definition                                                                 | VIDOC table(s)                                            | data.gouv.fr            | Tracking |
| ------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------- | --------------------------------------------------------- | ----------------------- | -------- |
| [Diversity rate (3+ categories)](https://pass-culture.github.io/data-gcp/dbt/kpis/diversity/diversity_3plus_categories/index.md)      | Share who booked in 3+ categories at credit expiration                     | `exp_vidoc_diversity`                                     | `diversity`             | Monthly  |
| [Diversity depth (1+ to 5+ categories)](https://pass-culture.github.io/data-gcp/dbt/kpis/diversity/diversity_category_depth/index.md) | Share who booked in at least N categories at credit expiration, N = 1 to 5 | `exp_vidoc_diversity`                                     | –                       | Monthly  |
| [Booking share by category](https://pass-culture.github.io/data-gcp/dbt/kpis/diversity/booking_share_by_category/index.md)            | Share who booked in category X at credit expiration                        | `exp_vidoc_diversity_by_category` + `exp_vidoc_diversity` | `diversity_by_category` | Monthly  |
| [Distinct venues booked](https://pass-culture.github.io/data-gcp/dbt/kpis/diversity/distinct_venues_booked/index.md)                  | Average number of distinct venues booked per beneficiary at expiration     | `exp_vidoc_diversity`                                     | –                       | Monthly  |
