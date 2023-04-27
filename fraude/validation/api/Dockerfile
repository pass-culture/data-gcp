
FROM python:3.9

# Install libraries
COPY ./requirements.txt ./
RUN pip3 install -r requirements.txt 

# Setup container directories
RUN mkdir /api

# Copy local code to the container
COPY ./api /api

WORKDIR /api
EXPOSE 8080
CMD ["gunicorn", "main:app", "--preload", "--timeout=60", "--workers=4", "--threads=2", "--bind=0.0.0.0:8080"]

