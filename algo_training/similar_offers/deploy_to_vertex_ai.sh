cp -rf ${TRAIN_DIR}/${ENV_SHORT_NAME}/tf_reco ./model/

gcloud auth configure-docker 
docker build . -t $API_DOCKER_IMAGE
docker push $API_DOCKER_IMAGE
