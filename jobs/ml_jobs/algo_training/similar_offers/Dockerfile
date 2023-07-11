
FROM python:3.9

COPY ./api-requirements.txt ./
RUN pip3 install -r api-requirements.txt 


RUN mkdir /app
COPY ./app /app
COPY ./metadata /app/metadata


WORKDIR /app
EXPOSE 8080
CMD ["gunicorn", "main:app", "--preload", "--timeout=60", "--workers=4", "--bind=0.0.0.0:8080"]
