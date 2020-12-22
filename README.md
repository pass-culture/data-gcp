# data-gcp

Repo pour la team data sur GCP

## Organisation

```
+-- recommendation:   
|
|   +-- api : l'api de recommandation déployée sur Cloud Run   
|
|   +-- model : modèle de l'algorithme de recommandation
|
|   +-- db : les scripts sql/python qui concernent le cloudsql de recommandation
|
+-- analytics : l'ensemble du code (utilitaires, configuration, scripts) pour interagir avec BigQuery
|
+-- orchestration : les dags pour Cloud Composer
```

## INSTALL
### Analytics (BigQuery)
**Prérequis** :
- pyenv
- poetry
- accès aux comptes de services GCP

**1. Configure le projet**

```
git clone git@github.com:pass-culture/data-gcp.git
cd data-gcp
pyenv install 3.7.7  # si nécessaire
pyenv local 3.7.7
poetry install
```

**2. Configure le compte de service GCP**

Crée une clé sur un compte de service existant (ou nouveau) et télécharge la en JSON.

(ou copie le fichier présent dans le 1password ("compte de service [Bigquery][test]") pour des droits par défaut sur BigQuery)

Copie le fichier `.env` en `.env.local`.

Dans `.env.local`, renseigne le chemin vers ta clé (.JSON) dans la variable `GOOGLE_APPLICATION_CREDENTIALS`.

**3. Ajoute analytics au PYTHONPATH**
```
export PYTHONPATH=$PYTHONPATH:"path/to/analytics"
```

Vous pouvez maintenant lancer les différents scripts !

## Cloud Composer

[plus de détails dans dags/README.md](/orchestration/dags/README.md)

UI Airflow disponible ici : https://q775b71be829eada6p-tp.appspot.com/admin/

Les dags sont déployés automatiquement lors d'un merge sur master.


## Tests CloudSQL

On run les tests de CloudSQL avec la commande suivante : `./run_tests.sh`

Commande pour démarrer le docker postgres de test :
`docker-compose up --build`

Auquel on peut se connecter avec la commande suivante:
`psql -h localhost -p 5432 -d postgres -U postgres`

Le mot de passe est 'postgres'.

Pour lancer les tests une fois le container démarré :
`poetry run pytest`

Pour lancer les tests avec plus de détails :
`poetry run pytest -s`

## CI/CD
### CI
On utilise CircleCI pour lancer des tests sur les différentes parties du repo.
Les tests sont lancés sur toutes les branches git et sont réparties entre les jobs suivants :
- *linter* : tester le bon formattage du code de tout le repo en utilisant `Black`
- *analytics-tests* : tester les requêtes de création des tables enrichies
- *recommendation-db-tests* : tester l'ingestion des données dans le CloudSQL pour l'algorithme de recommendation
- *recommendation-api-tests* : tester l'API de recommendation
- *orchestration-tests* : tester les différents DAGs d'orchestration
