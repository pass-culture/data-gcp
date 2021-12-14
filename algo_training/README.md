## Algo Training

Les scripts d'entraînement de l'algorithme sont exécutées via le DAG Airflow `orchestration/dags/algo_training.py`, sur
une machine Google Compute Engine :

- algo-training-dev (projet passculture-data-ehp)
- algo-training-stg (projet passculture-data-ehp )
- algo-training-prod (projet passculture-data-prod)

NB: verifier que la VM sur laquelle on effectue le test a bien les droits de lecture sur les tables requêter pour récupérer la donnée d'entrainement.

On peut les tester de la façon suivante (les commandes sont écrites pour l'environnement de dev) :

- démarrer la VM GCE
- on se ssh sur la VM correspondante depuis la cloudshell : `gcloud compute ssh airflow@algo-training-dev`
- on définit les variables d'environnement nécessaires :
  - `export STORAGE_PATH=gs://data-bucket-dev/test_training`
  - `export ENV_SHORT_NAME=ehp`
  - `export GCP_PROJECT_ID=passculture-data-ehp`
- on pull la dernière version du code : `git pull`
- on se place sur la branche où les nouvelles features sont en développement : `git checkout <nom-de-ma-branche>`
- on exécute les scripts exécutés par Airflow : `python data_collect.py`, `python preprocess.py`
- on vérifie les logs sur MLflow :
  - pour le projet ehp : [mlflow-ehp.internal-passculture.app](https://mlflow-ehp.internal-passculture.app)
  - pour le projet prod : [mlflow.internal-passculture.app](https://mlflow.internal-passculture.app)
  - [Notion](https://www.notion.so/passcultureapp/Mlflow-1dbb2d3ec71e43cb871a5c389b79e753#bfa1e789cfd245e79bd6f2cecd11deda)

## Test du DAG algo_training en dev

Afin de tester le code en développement, il faut modifier le nom de la branche utilisée en développement dans le DAG `algo_training.py`:

```python
import os

ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")

if ENV_SHORT_NAME == "dev":
    branch = "PC-9207-debug-mlops"
if ENV_SHORT_NAME == "stg":
    branch = "master"
if ENV_SHORT_NAME == "prod":
    branch = "production"
```

De plus, afin de réduire les temps d'entraînement et d'évaluation, on peut réduire le volume de donnée utilisé en sélectionnant les 10 derniers jours dans `algo_training/utils.py`
Par défaut la valeur est de 10 en dev et de 150 en staging et production :

```python
import os

ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME")
BOOKING_DAY_NUMBER = 10 if ENV_SHORT_NAME == "dev" else 5*30
```
