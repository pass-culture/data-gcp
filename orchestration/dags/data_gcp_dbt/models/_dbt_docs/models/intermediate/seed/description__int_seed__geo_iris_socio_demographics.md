---
title: Socio Demographic Date Per IRIS
description: Description of the `int_seed__geo_iris_socio_demographics` table.
---

{% docs description__int_seed__geo_iris_socio_demographics %}

# Table: Socio Demographics Per IRIS

This model provides socio-demographic statistics for France at the IRIS level, based on INSEE datasets from the 2020 population census, FiLoSoFi, and other public datasets. It includes breakdowns by age group, socio-professional categories (CSP), nationality, and income quantiles.

The data was aggregated and cleaned via an intermediate Jupyter notebook that harmonizes multiple sources into a single, structured dataset. Each row represents an IRIS unit, which is the smallest statistical geographic unit used by INSEE.

{% enddocs %}

## Table description

Data sources :
    - Code Commune / Canton (INSEE)
         A dataset containing codes for French communes and cantons, enabling the geographical linking of socio-economic data. This source is key for geographical identification in data analyses.
         [Link to source](https://www.insee.fr/fr/information/7766585)
    - Population 2020 et Statistiques (INSEE)
         The 2020 census data, detailing demographic characteristics like age, sex, and socio-professional categories. It is used to analyze the population structure in France.
         [Link to source](https://www.insee.fr/fr/statistiques/7704076#dictionnaire)
    - FiLoSoFi IRIS ou COM - 2020 (INSEE)
         This dataset provides detailed information on the living standards of households at a fine geographical level (IRIS or communes), focusing on income and poverty levels. It enables a detailed analysis of income distribution and economic disparities.
         [Link to FiLoSoFi IRIS 2020](https://www.insee.fr/fr/statistiques/8229323#documentation)
         [Link to FiLoSoFi IRIS ou COM - 2020](https://www.insee.fr/fr/statistiques/6692392?sommaire=6692394#consulter)
    - Population 2020 (INSEE)
         Detailed demographic data for the population of France in 2020, covering age, sex, nationality, and other socio-demographic factors. This data is used for socio-demographic analysis and infrastructure planning.
         [Link to source](https://www.insee.fr/fr/statistiques/7704076#consulter)

{% docs table__int_seed__geo_iris_socio_demographics %}{% enddocs %}
