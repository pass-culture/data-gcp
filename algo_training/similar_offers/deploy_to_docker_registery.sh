#!/bin/bash 
TRAIN_DIR=$1
ENV_SHORT_NAME=$2
API_DOCKER_IMAGE=$3

echo "${API_DOCKER_IMAGE}"
cp -rf ${TRAIN_DIR}/${ENV_SHORT_NAME}/model ./model/

gcloud auth configure-docker 
docker build . -t $API_DOCKER_IMAGE
docker push $API_DOCKER_IMAGE

echo "Done !"