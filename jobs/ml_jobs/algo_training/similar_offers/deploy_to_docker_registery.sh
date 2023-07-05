#!/bin/bash 
API_DOCKER_IMAGE=$1

echo "${API_DOCKER_IMAGE}"

gcloud auth configure-docker 
docker build . -t $API_DOCKER_IMAGE
docker push $API_DOCKER_IMAGE

echo "Done !"