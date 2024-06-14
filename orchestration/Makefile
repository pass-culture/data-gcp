build_and_init:
	docker-compose  build --no-cache;
	docker-compose up -d;

start:
	docker-compose up -d;

stop:
	docker-compose stop;

rebuild:
	docker-compose down;
	docker-compose build;
	docker-compose up -d;

show_airflow_logs:
	docker-compose logs -f;
