
FROM python:3.9

# Install libraries
COPY ./api-requirements.txt ./
RUN pip3 install -r api-requirements.txt 

# Setup container directories
RUN mkdir /app

# Copy local code to the container
COPY ./app /app

COPY ./model /app/model


WORKDIR /app
EXPOSE 8080
CMD ["gunicorn", "main:app", "--preload", "--timeout=60", "--workers=4", "--threads=2", "--bind=0.0.0.0:8080"]

