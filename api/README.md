# API Recommandation

Code source de l'api de recommandation.
API Flask avec gunicorn déployée dans Cloud Run.

Api crée en suivant : https://cloud.google.com/run/docs/quickstarts/build-and-deploy?hl=fr#python


## Routes
- Adresse API : https://apireco-4di2kltlja-ew.a.run.app/

- Route de check : https://apireco-4di2kltlja-ew.a.run.app/check



## Utils

### Pour conteneuriser l'image

```
cd api
gcloud builds submit --tag gcr.io/<PROJECT-ID>/<IMAGE-NAME>
```
- PROJECT-ID : L'id du projet (pass-culture-app-projet-test)
- IMAGE-NAME : Le nom de l'image (api_reco)

### Pour déployer sur Cloud Run

**Premier déployement**
```
cd api
gcloud run deploy <SERVICE> --image <IMAGE> --platform managed
```
- SERVICE : nom du service Cloud Run a créer (apireco)
- IMAGE : L'url de l'image à déployer (gcr.io/pass-culture-app-projet-test/api_reco)

**Pour déployer une nouvelle version**
```
cd api
gcloud run deploy <SERVICE> --image <IMAGE>:latest
```
- SERVICE : nom du service Cloud Run a redéployer (apireco)
- IMAGE : L'url de l'image à déployer (gcr.io/pass-culture-app-projet-test/api_reco)
