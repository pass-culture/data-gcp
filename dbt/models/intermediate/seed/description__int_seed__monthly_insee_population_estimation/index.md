This model provides an estimation of the French population by age, department, and birth month. The data is produced by the [`data-insee-population`](https://github.com/pass-culture/data-insee-population/) project, which builds the seed loaded here.

**Global logic (simplified):**

1. Start from the 2022 INSEE census as a baseline of "who lives where" by age and geography.
1. Project each birth cohort forward year by year, keeping its geographic distribution stable (i.e. the share of a given age group living in each department stays close to what the census observed).
1. Anchor totals to INSEE's annual departmental population estimates so the projection stays aligned with official figures.
1. Split each yearly cohort into months using regional month-of-birth patterns (and national birth statistics as a fallback) to get a monthly granularity.
1. Adjust ages 15–24 with student mobility data (RP2022 MOBSCO) to better reflect where young adults actually live.

The detailed methodology is documented in the [`data-insee-population` method doc](https://github.com/pass-culture/data-insee-population/blob/main/docs/method.md).

**Data reliability:**

- Reliable at the regional and annual age level.
- Relatively accurate at the departmental level for areas with low population mobility (ages 15-20).
- Less precise in departments with low population volumes.
- Monthly birth estimates are approximate but reflect seasonal trends.

**Related models:**

- [`metrics_population__coverage`](#!/model/model.data_gcp_dbt.metrics_population__coverage) — central population coverage indicators built on top of this seed.
- [`exp_vidoc_population__coverage`](#!/model/model.data_gcp_dbt.exp_vidoc_population__coverage) — export of those indicators for the ministry's vidoc visualisation.
