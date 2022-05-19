
# Use the official lightweight Python image.
# https://hub.docker.com/_/python
FROM python:3.7-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY src/app.py ./
COPY api-requirements.txt ./
COPY src/pcreco/core/user.py pcreco/core/
COPY src/pcreco/core/scoring.py pcreco/core/
COPY src/pcreco/core/utils/* pcreco/core/utils/
COPY src/pcreco/models/reco/* pcreco/models/reco/
COPY src/pcreco/utils/db/* pcreco/utils/db/
COPY src/pcreco/utils/secrets/* pcreco/utils/secrets/
COPY src/pcreco/utils/* pcreco/utils/

# Install production dependencies.
RUN pip install -r api-requirements.txt

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 app:app
