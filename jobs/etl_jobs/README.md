# ETL jobs

## Fonctionnement

Les jobs ETL sont des scripts python qui permettent de récupérer des données depuis différentes sources, de les transformer et de les charger dans BigQuery. Cest Jobs sont appelés par les DAGs Airflow.

## Découpage

Les jobs ETL sont divisés en 2 catégories :

- `external` : Ce sont les jobs qui appellent des APIs externes pour récupérer des données
- `internal` : Ce sont les jobs qui appellent des services internes au Pass (services GCP ou API internes)

** Remarque** : Le découpage n'est pas 100% respecté dans le code existant, mais c'est l'objectif à atteindre.
