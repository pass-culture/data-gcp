## Algo Training

Les scripts d'entraînement de l'algorithme sont exécutées via la dag Airflow `orchestration/dags/algo_training.py`, sur
une machine Google Cloud Compute : 
- algo-training-dev (projet passculture-data-ehp)
- algo-training-stg (projet passculture-data-ehp )
- algo-training-prod (projet passculture-data-prod)

On peut les tester de la façon suivante (les commandes sont écrites pour l'environnement de dev) :
- démarrer la VM Compute
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
  