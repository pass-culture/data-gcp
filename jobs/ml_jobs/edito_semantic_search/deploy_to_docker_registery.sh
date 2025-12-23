#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

API_DOCKER_IMAGE=$1
WORKERS=$2

echo "${API_DOCKER_IMAGE}"
echo "${WORKERS}"

yes | gcloud auth configure-docker europe-west1-docker.pkg.dev
docker build . -t $API_DOCKER_IMAGE --build-arg WORKERS=$WORKERS
docker push $API_DOCKER_IMAGE

echo "Done !"
