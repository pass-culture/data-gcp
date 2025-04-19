#!/bin/bash
API_DOCKER_IMAGE=$1

echo "${API_DOCKER_IMAGE}"

yes | gcloud auth configure-docker europe-west1-docker.pkg.dev
docker build . -t $API_DOCKER_IMAGE
docker push $API_DOCKER_IMAGE

echo "Done !"
