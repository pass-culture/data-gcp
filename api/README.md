# API Recommendation

Code source de l'api de recommendation

API Flask avec gunicorn déployée dans Cloud Run.

Creation de l'api faite en suivant : https://cloud.google.com/run/docs/quickstarts/build-and-deploy?hl=fr#python

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
