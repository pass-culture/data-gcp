FROM python:3.9-slim

ENV PYTHONUNBUFFERED True

ENV APP_HOME /app
WORKDIR $APP_HOME
# Install libraries
COPY ./requirements.txt ./
COPY ./requirements_no_deps.txt ./
RUN pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir --no-deps -r requirements_no_deps.txt 
COPY ./src/main.py .
COPY ./src .
# Copy local code to the container
COPY ./src/pcvalidation ./pcvalidation

CMD exec gunicorn --bind :$PORT --workers 1 --threads 10 --preload --timeout 0 main:app