FROM python:3.9-slim

ENV PYTHONUNBUFFERED True

ENV APP_HOME /app
WORKDIR $APP_HOME
# Install libraries
COPY ./requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy local code to the container
COPY ./src .
COPY ./src/huggy ./huggy
EXPOSE 8000
CMD exec gunicorn -k uvicorn.workers.UvicornWorker --bind :8000 --workers 1 --threads 10 --preload --timeout 0 main:app