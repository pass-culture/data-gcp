# Table: Socio Demographics Per IRIS

This model provides agregated socio-demographic statistics for France at the IRIS level, based on INSEE datasets from the 2020 population census, FiLoSoFi, and other public datasets. It includes breakdowns by age group, socio-professional categories (CSP), nationality, and income quantiles.

The data was aggregated and cleaned via an intermediate Jupyter notebook that harmonizes multiple sources into a single, structured dataset. Each row represents an IRIS unit, which is the smallest statistical geographic unit used by INSEE. It was then agregated with the global venue model to include a venue_density metric.

## Table description

Data sources :

- Code Commune / Canton (INSEE) A dataset containing codes for French communes and cantons, enabling the geographical linking of socio-economic data. This source is key for geographical identification in data analyses. [Link to source](https://www.insee.fr/fr/information/7766585)
- Population 2020 et Statistiques (INSEE) The 2020 census data, detailing demographic characteristics like age, sex, and socio-professional categories. It is used to analyze the population structure in France. [Link to source](https://www.insee.fr/fr/statistiques/7704076#dictionnaire)
- FiLoSoFi IRIS ou COM - 2020 (INSEE) This dataset provides detailed information on the living standards of households at a fine geographical level (IRIS or communes), focusing on income and poverty levels. It enables a detailed analysis of income distribution and economic disparities. [Link to FiLoSoFi IRIS 2020](https://www.insee.fr/fr/statistiques/8229323#documentation) [Link to FiLoSoFi IRIS ou COM - 2020](https://www.insee.fr/fr/statistiques/6692392?sommaire=6692394#consulter)
- Population 2020 (INSEE) Detailed demographic data for the population of France in 2020, covering age, sex, nationality, and other socio-demographic factors. This data is used for socio-demographic analysis and infrastructure planning. [Link to source](https://www.insee.fr/fr/statistiques/7704076#consulter)

| name                              | data_type | description                                                                                                                                                    |
| --------------------------------- | --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| iris_code                         |           | The official INSEE code identifying the IRIS (Îlots Regroupés pour l'Information Statistique) area.                                                            |
| iris_internal_id                  |           | The internal identifier for the IRIS area within the system.                                                                                                   |
| iris_label                        |           | The descriptive name or label of the IRIS area.                                                                                                                |
| city_code                         |           | The official INSEE code of the municipality (commune) containing the IRIS area.                                                                                |
| city_label                        |           | The name of the municipality (commune) containing the IRIS area.                                                                                               |
| territorial_authority_code        |           | The official code of the territorial authority (collectivité territoriale) containing the IRIS area.                                                           |
| epci_code                         |           | The official code of the EPCI (Établissement Public de Coopération Intercommunale) containing the IRIS area.                                                   |
| epci_label                        |           | The name of the EPCI containing the IRIS area.                                                                                                                 |
| department_code                   |           | The official code of the department (département) containing the IRIS area.                                                                                    |
| department_name                   |           | The official name of the department containing the IRIS area.                                                                                                  |
| region_name                       |           | The official name of the region containing the IRIS area.                                                                                                      |
| academy_name                      |           | The name of the educational academy (académie) associated with the IRIS area.                                                                                  |
| territorial_authority_label       |           | The name of the territorial authority (collectivité territoriale) containing the IRIS area.                                                                    |
| density_level                     |           | The numerical level indicating the population density of the IRIS area.                                                                                        |
| density_label                     |           | The descriptive label for the population density level of the IRIS area.                                                                                       |
| density_macro_level               |           | The broader categorization of population density for the IRIS area.                                                                                            |
| geo_code                          |           | The unique geographic code identifying the IRIS area.                                                                                                          |
| rural_city_type                   |           | The classification of the municipality type (rural, urban, etc.) containing the IRIS area.                                                                     |
| iris_area_sq_km                   |           | The area of the IRIS, calculated in square kilometers.                                                                                                         |
| total_venue_20_km                 |           | The total distinct number of venues located under a 20 kilometers area around the centroid of the IRIS.                                                        |
| total_venue_5_km                  |           | The total distinct number of venues located under a 5 kilometers area around the centroid of the IRIS.                                                         |
| total_population                  |           | The total number of french population.                                                                                                                         |
| total_population_15_years_or_more |           | The total population over 15 years living in the IRIS.                                                                                                         |
| total_population_11_17_years      |           | The total population between 11 and 17 years living in the IRIS.                                                                                               |
| total_population_18_24_years      |           | The total population between 18 ans 24 years living in the IRIS.                                                                                               |
| total_revenue_10_decile           |           | The 10th percentile of income for the IRIS area, representing the income below which 10% of the population earns.                                              |
| total_revenue_50_decile           |           | The 50th percentile of income for the IRIS area, representing the median income.                                                                               |
| total_revenue_90_decile           |           | The 90th percentile of income for the IRIS area, representing the income below which 90% of the population earns.                                              |
| pct_csp_1                         |           | The percentage of the population, that have 15 years or more, belonging to the socio-professional category 1 - Farmers.                                        |
| pct_csp_2                         |           | The percentage of the population, that have 15 years or more, belonging to the socio-professional category 2 - Craftsmen, Traders, Business Leaders.           |
| pct_csp_3                         |           | The percentage of the population, that have 15 years or more, belonging to the socio-professional category 3 - Executives and Higher Intellectual Professions. |
| pct_csp_4                         |           | The percentage of the population, that have 15 years or more, belonging to the socio-professional category 4 - Intermediate professions.                       |
| pct_csp_5                         |           | The percentage of the population, that have 15 years or more, belonging to the socio-professional category 5 - Employees.                                      |
| pct_csp_6                         |           | The percentage of the population, that have 15 years or more, belonging to the socio-professional category 6 - Workers.                                        |
| pct_csp_7                         |           | The percentage of the population, that have 15 years or more, belonging to the socio-professional category 7 - Retired.                                        |
| pct_csp_8                         |           | The percentage of the population, that have 15 years or more, belonging to the socio-professional category 8 - Others without professional activity.           |
| pct_french_population             |           | The percentage of the population that have French nationality, all ages included.                                                                              |
| pct_foreign_population            |           | The percentage of the population that have foreign nationality, all ages included.                                                                             |
