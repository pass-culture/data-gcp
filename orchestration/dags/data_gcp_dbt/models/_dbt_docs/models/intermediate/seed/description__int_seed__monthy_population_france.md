---
title: Population Estimation
description: Description of the `population_estimation` table.
---

{% docs description__population_estimation %}
This model provides an estimation of the French population by age, department, and birth month.
The data is sourced from INSEE and accounts for population movements and seasonal birth trends.

**Sources:**
1. Estimation de population par département, sexe et âge quinquennal (1975-202X): [INSEE](https://www.insee.fr/fr/statistiques/1893198)
2. Individus localisés au canton-ou-ville en 2019: [INSEE](https://www.insee.fr/fr/statistiques/6544333?sommaire=6456104&q=Individus+localis%C3%A9s+au+canton-ou-ville)
3. Naissances par département, par mois en France.

**Data reliability:**
- Reliable at the regional and annual age level.
- Relatively accurate at the departmental level for areas with low population mobility (ages 15-20).
- Less precise in departments with low population volumes.
- Monthly birth estimates are approximate but reflect seasonal trends.
{% enddocs %}
