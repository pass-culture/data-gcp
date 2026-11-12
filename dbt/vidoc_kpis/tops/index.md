## Tops

Monthly top 50 offers and cultural venues by used booking revenue, at national, regional and departmental levels.

**Data protection**: Only the first 50 entities of each ranking are exported; amounts are published per offer and per venue, as in the external reporting tops. Not part of the vidoc lot 1 export to the ministry. See [Statistical confidentiality](https://pass-culture.github.io/data-gcp/dbt/references/statistical_confidentiality/index.md).

| KPI                                                                                                                              | Definition                                                                      | Source table(s)                | Tracking |
| -------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- | ------------------------------ | -------- |
| [Top offers](https://pass-culture.github.io/data-gcp/dbt/vidoc_kpis/tops/top_offer_ranking/index.md)                             | Top 50 items by used booking revenue                                            | `exp_vidoc_top_offer`          | Monthly  |
| [Top offers by category](https://pass-culture.github.io/data-gcp/dbt/vidoc_kpis/tops/top_offer_category_ranking/index.md)        | Top 50 items by used booking revenue within each offer category                 | `exp_vidoc_top_offer_category` | Monthly  |
| [Top offers in labelled venues](https://pass-culture.github.io/data-gcp/dbt/vidoc_kpis/tops/top_offer_labelled_ranking/index.md) | Top 50 items by used booking revenue in venues with a Ministry of Culture label | `exp_vidoc_top_offer_labelled` | Monthly  |
| [Top venues](https://pass-culture.github.io/data-gcp/dbt/vidoc_kpis/tops/top_venue_ranking/index.md)                             | Top 50 venues by used booking revenue                                           | `exp_vidoc_top_venue`          | Monthly  |
| [Top labelled venues](https://pass-culture.github.io/data-gcp/dbt/vidoc_kpis/tops/top_venue_labelled_ranking/index.md)           | Top 50 venues with a Ministry of Culture label by used booking revenue          | `exp_vidoc_top_venue_labelled` | Monthly  |
