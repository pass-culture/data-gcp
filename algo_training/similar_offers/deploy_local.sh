#cp -rf ${TRAIN_DIR}/${ENV_SHORT_NAME}/tf_reco ./model/
docker build -t sim-offers .
docker run -it -p 8080:8080 sim-offers