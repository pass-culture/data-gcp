
FROM python:3.7

ARG TRAIN_DIR
ARG ENV_SHORT_NAME
# Install libraries
COPY ./requirements.txt ./
RUN pip install -r requirements.txt 

# Setup container directories
RUN mkdir /app
RUN mkdir -p /app/model/tf_reco
RUN mkdir -p /app/model/sim_offers

# Copy local code to the container
COPY ./app /app

COPY ./model/tf_reco /app/model/tf_reco
COPY ./model/sim_offers /app/model/sim_offers


WORKDIR /app
EXPOSE 8080
CMD ["gunicorn", "main:app", "--timeout=0", "--preload", "--workers=1", "--threads=2", "--bind=0.0.0.0:8080"]

