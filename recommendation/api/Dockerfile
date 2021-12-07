
# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.7-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY app.py ./
COPY geolocalisation.py ./
COPY health_check_queries.py ./
COPY recommendation.py ./
COPY access_gcp_secrets.py ./
COPY api-requirements.txt ./
COPY not_eac/cold_start.py ./
COPY eac/eac_cold_start.py ./
COPY not_eac/ab_testing.py ./
COPY eac/eac_ab_testing.py ./
COPY not_eac/scoring.py ./
COPY eac/eac_scoring.py ./
COPY utils.py ./

# Install production dependencies.
RUN pip install -r api-requirements.txt

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app
