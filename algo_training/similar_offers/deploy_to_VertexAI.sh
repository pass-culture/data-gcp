gcloud auth configure-docker 
docker build . -t $API_FOCKER_IMAGE
docker push $API_FOCKER_IMAGE
