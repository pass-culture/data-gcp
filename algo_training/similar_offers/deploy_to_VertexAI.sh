gcloud auth configure-docker 
docker build . -t eu.gcr.io/passculture-data-ehp/offres-similaires-api:dev-ready
docker push eu.gcr.io/passculture-data-ehp/offres-similaires-api:dev-ready
