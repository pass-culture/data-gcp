FROM python:3.10

# Install libraries
COPY ./requirements.txt ./
RUN pip install -r requirements.txt

# Setup container directories
RUN mkdir /src

# Copy local code to the container
COPY ./src ./src

WORKDIR /src
EXPOSE 8080
CMD ["gunicorn", "main:app", "--preload", "--timeout=60", "--workers=4", "--threads=2", "--bind=0.0.0.0:8080"]

