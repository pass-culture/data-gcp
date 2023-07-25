# API Recommandation

Code source de l'api de recommandation.
API Flask avec gunicorn déployée dans Cloud Run.

Api créée en suivant : https://cloud.google.com/run/docs/quickstarts/build-and-deploy?hl=fr#python


## Routes
- Recommandation : /playlist_recommendation/<user_id>?\<token>
  - token : dans le 1password

- Route de check : /check

- Health check de la bdd :
  - /health/recommendable_offers
  - /health/non_recommendable_offers


## Tests

### Tests unitaires

Pour lancer les tests :
```
pytest
```

Pour le test coverage :
```
pytest --cov
```
(les paramètres sont réglés dans le `.coveragerc`)

### Tests d'intégration

**Objectif:**
L'objectif des tests d'intégration est de vérifier le bon fonctionnement de l'api dans son ensemble. C'est à dire son fonctionnement ainsi que la communication entre l'api et les autres services (CloudSQL, AI Platform...).

**Fonctionnement:**
On utilise postman et newman pour faire ces tests d'intégration.
Les tests vont appeler toutes les routes de l'API et vérifier qu'elle renvoit ce qui est attendu.

**En pratique:**
Pour modifier les tests, ouvrir les fichiers du dossier `/postman` avec Postman, les modifier via l'UI, et les exporter de nouveau. (On peut aussi modifier directement les jsons mais ce n'est pas très lisible.)

Le dossier contient:
- \<env>.postman_environement.json: les valeurs des variables d'environnement (par ex : {api_url})
- api_integration_tests.postman_collection.json: Les appels et les tests à faire.




**Ressources:**
- Article de blog CircleCI + Newman: https://circleci.com/blog/testing-an-api-with-postman/
- Pour lancer les tests avec newman: https://learning.postman.com/docs/running-collections/using-newman-cli/command-line-integration-with-newman/
- Comment écrire des tests avec postman: https://learning.postman.com/docs/writing-scripts/test-scripts/
- Exemples de scripts de test: https://learning.postman.com/docs/writing-scripts/script-references/test-examples/


## Déploiement

>Le déploiement est fait **automatiquement** via un job CircleCI pour les environements de **staging** et de **production**.

------

**Etape 1:** Conteneuriser l'image

```
cd apps/recommendation/api
gcloud builds submit \
  --tag eu.gcr.io/<PROJECT-ID>/data-gcp/<IMAGE-NAME> \

```
- PROJECT-ID : L'id du projet (passculture-data-\<env>)
- IMAGE-NAME : Le nom de l'image (apireco-\<env>)

En dev ça donne:
```
gcloud builds submit --tag eu.gcr.io/passculture-data-ehp/data-gcp/apireco-dev
```


En stg ça donne:
```
gcloud builds submit --tag eu.gcr.io/passculture-data-ehp/data-gcp/apireco-stg
```

-------

**Etape 2:** Déployer une révision sur Cloud Run

>Si demandé toujours choisir les options:
>- target platform: "Cloud Run (fully managed)"
>- region: "europe-west1"


```
cd apps/recommendation/api

gcloud run deploy <SERVICE> \
--image <IMAGE>:latest \
--region europe-west1 \
--allow-unauthenticated \
--platform managed
```
- SERVICE : nom du service Cloud Run a redéployer (apireco-\<env>)
- IMAGE : L'url de l'image à déployer (eu.gcr.io/passculture-data-\<env>/data-gcp/api_reco)

En dev ça donne:
```
gcloud run deploy apireco-dev \
--image eu.gcr.io/passculture-data-ehp/data-gcp/apireco-dev:latest \
--region europe-west1 \
--allow-unauthenticated \
--platform managed
```

En stg ça donne:
```
gcloud run deploy apireco-stg \
--image eu.gcr.io/passculture-data-ehp/data-gcp/apireco-stg:latest \
--region europe-west1 \
--allow-unauthenticated \
--platform managed
```


## Infos utiles

- Les variables d'environnement sont définies dans le code terraform du cloud run.
