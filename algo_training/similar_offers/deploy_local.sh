cp -rf ${TRAIN_DIR}/${ENV_SHORT_NAME}/tf_reco ./model/
cp -rf ${TRAIN_DIR}/${ENV_SHORT_NAME}/sim_offers ./model/

docker build -t sim-offers .
docker run -it -p 8080:8080 sim-offers