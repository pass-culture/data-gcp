# API Recommendation

Code source de l'api de recommendation

API Flask avec gunicorn déployée dans Cloud Run.

Creation de l'api faite en suivant : https://cloud.google.com/run/docs/quickstarts/build-and-deploy?hl=fr#python


Pour build l'image :
```
gcloud builds submit --tag gcr.io/pass-culture-app-projet-test/api_reco
```

Puis pour la déployer sur Cloud Run :
```
gcloud run deploy --image gcr.io/pass-culture-app-projet-test/api_reco --platform managed
```