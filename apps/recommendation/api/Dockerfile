FROM python:3.7-slim

ENV PYTHONUNBUFFERED True

ENV APP_HOME /app
WORKDIR $APP_HOME
COPY src/app.py ./
COPY api-requirements.txt ./

COPY src/pcreco/ ./pcreco/

RUN pip install -r api-requirements.txt

CMD exec gunicorn --bind :$PORT --workers 1 --threads 10 --preload --timeout 0 app:app
