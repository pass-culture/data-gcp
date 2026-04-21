#!/bin/bash
set -e  # Exit immediately if a command exits with a non-zero status

API_DOCKER_IMAGE=$1
WORKERS=$2
GCP_PROJECT=$3
ENV_SHORT_NAME=$4

echo "${API_DOCKER_IMAGE}"
echo "${WORKERS}"
echo "${GCP_PROJECT}"
echo "${ENV_SHORT_NAME}"
yes | gcloud auth configure-docker europe-west1-docker.pkg.dev
docker build . \
	-t $API_DOCKER_IMAGE \
	--build-arg WORKERS=$WORKERS \
	--build-arg GCP_PROJECT=$GCP_PROJECT \
	--build-arg ENV_SHORT_NAME=$ENV_SHORT_NAME
docker push $API_DOCKER_IMAGE

echo "Done !"
