install-composer-dev:
	pip install git+https://github.com/GoogleCloudPlatform/composer-local-dev.git

create-airflow-local-dev:
	composer-dev create airflow-local-dev \
    --from-source-environment data-composer-dev \
    --location europe-west1 \
    --project passculture-data-ehp \
    --port 8080 \
    --dags-path dags

start-airflow-local-dev:
	composer-dev start airflow-local-dev

restart-airflow-local-dev:
	composer-dev restart airflow-local-dev

stop-airflow-local-dev:
	composer-dev start airflow-local-dev
