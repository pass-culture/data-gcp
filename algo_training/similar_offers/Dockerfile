
FROM python:3.7

# Install libraries
COPY ./requirements.txt ./
RUN pip install --user annoy
RUN pip install -r requirements.txt 

# Setup container directories
RUN mkdir /app

# Copy local code to the container
COPY ./app /app
COPY ./app/utils.py /app/utils.py
COPY ./app/model/* /app/model/
COPY ./app/model/sim_offers.ann ./app/model/
COPY ./app/model/tf_reco/* /app/model/tf_reco/
COPY ./app/model/tf_reco/variables/* /app/model/tf_reco/variables/
# launch server with gunicorn
WORKDIR /app
EXPOSE 8080
CMD ["gunicorn", "main:app", "--timeout=0", "--preload", "--workers=1", "--threads=4", "--bind=0.0.0.0:8080"]