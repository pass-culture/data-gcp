
FROM python:3.10

ARG WORKERS=5

ENV VIRTUAL_ENV=/home/packages/.venv
ADD https://astral.sh/uv/install.sh /install.sh
RUN chmod -R 655 /install.sh && /install.sh && rm /install.sh


COPY ./api-requirements.txt ./
RUN /root/.cargo/bin/uv pip install --no-cache -r api-requirements.txt --system

COPY ./app /app
COPY ./metadata /metadata
WORKDIR /
EXPOSE 8080
ENV WORKERS=${WORKERS}

CMD hypercorn --bind 0.0.0.0:8080 app.app:app --workers=${WORKERS}
