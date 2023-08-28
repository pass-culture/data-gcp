
FROM python:3.10

ARG WORKERS=3


COPY ./api-requirements.txt ./
RUN pip3 install -r api-requirements.txt 

COPY ./app /app
COPY ./metadata /app/metadata
WORKDIR /app
EXPOSE 8080
ENV WORKERS=${WORKERS}

CMD hypercorn --bind 0.0.0.0:8080 app:app --workers=${WORKERS}
