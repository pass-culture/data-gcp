build:
	docker-compose down -v;
	docker-compose build --no-cache;
	docker-compose up -d;

build_with_cache:
	docker-compose down -v;
	docker-compose build;
	docker-compose up -d;

start:
	docker-compose up -d;

stop:
	docker-compose stop;

show_airflow_logs:
	docker-compose logs -f;
