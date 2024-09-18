build:
	docker-compose down -v;
	docker-compose build --no-cache;
	docker-compose up -d;

build_with_cache:
	docker-compose down -v;
	docker-compose build;
	docker-compose up -d;

start:
	docker-compose up -d
	@echo "Waiting for Airflow to start..."
	@timeout 30 sh -c 'until curl -s http://0.0.0.0:8080 > /dev/null; do sleep 2; done' || (echo "Airflow did not start in time. Exiting." && exit 1)
	@echo "Airflow is ready!"
	python -m webbrowser "http://0.0.0.0:8080"

stop:
	docker-compose stop;

show_airflow_logs:
	docker-compose logs -f;
