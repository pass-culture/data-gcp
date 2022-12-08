init-dev:
	cp ./.env.default ./.env; \
	cp ./airflow/config/airflow.cfg.default ./airflow/config/airflow.cfg;

dev:
	export LOCAL_ENV="1"	
	docker-compose up -d;

stop:
	docker-compose stop;

build:
	docker-compose  build --no-cache;
