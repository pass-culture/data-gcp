# Table: Socio Demographics Per IRIS

This model provides socio-demographic statistics for France at the IRIS level, based on INSEE datasets from the 2020 population census, FiLoSoFi, and other public datasets. It includes breakdowns by age group, socio-professional categories (CSP), nationality, and income quantiles.

The data was aggregated and cleaned via an intermediate Jupyter notebook that harmonizes multiple sources into a single, structured dataset. Each row represents an IRIS unit, which is the smallest statistical geographic unit used by INSEE.

## Table description

Data sources :

- Code Commune / Canton (INSEE) A dataset containing codes for French communes and cantons, enabling the geographical linking of socio-economic data. This source is key for geographical identification in data analyses. [Link to source](https://www.insee.fr/fr/information/7766585)
- Population 2020 et Statistiques (INSEE) The 2020 census data, detailing demographic characteristics like age, sex, and socio-professional categories. It is used to analyze the population structure in France. [Link to source](https://www.insee.fr/fr/statistiques/7704076#dictionnaire)
- FiLoSoFi IRIS ou COM - 2020 (INSEE) This dataset provides detailed information on the living standards of households at a fine geographical level (IRIS or communes), focusing on income and poverty levels. It enables a detailed analysis of income distribution and economic disparities. [Link to FiLoSoFi IRIS 2020](https://www.insee.fr/fr/statistiques/8229323#documentation) [Link to FiLoSoFi IRIS ou COM - 2020](https://www.insee.fr/fr/statistiques/6692392?sommaire=6692394#consulter)
- Population 2020 (INSEE) Detailed demographic data for the population of France in 2020, covering age, sex, nationality, and other socio-demographic factors. This data is used for socio-demographic analysis and infrastructure planning. [Link to source](https://www.insee.fr/fr/statistiques/7704076#consulter)

| name                            | data_type | description                                                                                                                                           |
| ------------------------------- | --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| iris_code                       | STRING    | The official INSEE code identifying the IRIS (Îlots Regroupés pour l'Information Statistique) area.                                                   |
| geo_code                        | STRING    | The unique geographic code identifying the IRIS area.                                                                                                 |
| total_population                | FLOAT64   | The total number of french population.                                                                                                                |
| population_0_2_years            | FLOAT64   | The population of the IRIS area aged between 0 and 2 years old.                                                                                       |
| population_3_5_years            | FLOAT64   | The population of the IRIS area aged between 3 and 5 years old.                                                                                       |
| population_6_10_years           | FLOAT64   | The population of the IRIS area aged between 6 and 10 years old.                                                                                      |
| population_11_17_years          | FLOAT64   | The population of the IRIS area aged between 11 and 17 years old.                                                                                     |
| population_18_24_years          | FLOAT64   | The population of the IRIS area aged between 18 and 24 years old.                                                                                     |
| population_25_39_years          | FLOAT64   | The population of the IRIS area aged between 25 and 39 years old.                                                                                     |
| population_40_54_years          | FLOAT64   | The population of the IRIS area aged between 40 and 54 years old.                                                                                     |
| population_55_64_years          | FLOAT64   | The population of the IRIS area aged between 55 and 64 years old.                                                                                     |
| population_65_79_years          | FLOAT64   | The population of the IRIS area aged between 65 and 79 years old.                                                                                     |
| population_over_80_years        | FLOAT64   | The population of the IRIS area aged 80 years or older.                                                                                               |
| population_15_years_or_more     | FLOAT64   | The population of the IRIS area aged 15 years or older.                                                                                               |
| csp_1                           | FLOAT64   | The percentage of the population in the IRIS area employed in the highest socio-professional categories (e.g., managers, intellectual professionals). |
| csp_2                           | FLOAT64   | The percentage of the population in the IRIS area employed in intermediate socio-professional categories.                                             |
| csp_3                           | FLOAT64   | The percentage of the population in the IRIS area employed in the lower socio-professional categories (e.g., clerks, skilled workers).                |
| csp_4                           | FLOAT64   | The percentage of the population in the IRIS area employed in lower-skilled, manual occupations.                                                      |
| csp_5                           | FLOAT64   | The percentage of the population in the IRIS area employed in unskilled occupations.                                                                  |
| csp_6                           | FLOAT64   | The percentage of the population in the IRIS area employed in agricultural occupations.                                                               |
| csp_7                           | FLOAT64   | The percentage of the population in the IRIS area employed in industrial occupations.                                                                 |
| csp_8                           | FLOAT64   | The percentage of the population in the IRIS area employed in service occupations.                                                                    |
| french_population               | FLOAT64   | The total population in the IRIS area who are French nationals.                                                                                       |
| foreign_population              | FLOAT64   | The total population in the IRIS area who are foreign nationals.                                                                                      |
| immigrant_population            | FLOAT64   | The population in the IRIS area who are immigrants or first-generation foreigners.                                                                    |
| household_population            | FLOAT64   | The population in the IRIS area living in households.                                                                                                 |
| non_household_population        | FLOAT64   | The population in the IRIS area living in non-household settings, such as institutions or other group living arrangements.                            |
| department_code                 | STRING    | The official code of the department (département) containing the IRIS area.                                                                           |
| qt10_revenue                    | FLOAT64   | The 10th percentile of income for the IRIS area, representing the income below which 10% of the population earns.                                     |
| qt50_revenue                    | FLOAT64   | The 50th percentile of income for the IRIS area, representing the median income.                                                                      |
| qt90_revenue                    | FLOAT64   | The 90th percentile of income for the IRIS area, representing the income below which 90% of the population earns.                                     |
| interdecile_D9_D1_ratio         | FLOAT64   | The ratio between the 90th percentile and the 10th percentile of income, indicating income inequality within the IRIS area.                           |
| activity_income_share           | FLOAT64   | The share of total income derived from economic activity (e.g., employment, self-employment) in the IRIS area.                                        |
| wages_salaries_share            | FLOAT64   | The share of total income derived from wages and salaries in the IRIS area.                                                                           |
| unemployment_benefits_share     | FLOAT64   | The share of total income derived from unemployment benefits in the IRIS area.                                                                        |
| self_employment_income_share    | FLOAT64   | The share of total income derived from self-employment in the IRIS area.                                                                              |
| pensions_retirement_rents_share | FLOAT64   | The share of total income derived from pensions, retirement benefits, and rents in the IRIS area.                                                     |
| patrimony_income_share          | FLOAT64   | The share of total income derived from patrimony (e.g., investments, savings) in the IRIS area.                                                       |
| social_benefits_share           | FLOAT64   | The share of total income derived from social benefits in the IRIS area.                                                                              |
| family_benefits_share           | FLOAT64   | The share of total income derived from family benefits in the IRIS area.                                                                              |
| minimum_social_benefits_share   | FLOAT64   | The share of total income derived from minimum social benefits (e.g., welfare, allowances) in the IRIS area.                                          |
| housing_benefits_share          | FLOAT64   | The share of total income derived from housing benefits in the IRIS area.                                                                             |
| tax_share                       | FLOAT64   | The share of total income paid as taxes in the IRIS area.                                                                                             |
