FROM python:3.9.1

ENV PYTHONUNBUFFERED True

ENV APP_HOME /app
ENV PORT=8080
WORKDIR $APP_HOME
# Install libraries
COPY ./requirements.txt ./
RUN pip install -r requirements.txt

COPY ./src/main.py .
COPY ./src .
# Copy local code to the container
COPY ./src/pcvalidation ./pcvalidation

CMD exec gunicorn --bind :$PORT --workers 1 --threads 10 --preload --timeout 0 main:app