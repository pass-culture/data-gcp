#!/bin/bash

export PYTHONPATH=$PYTHONPATH:$(pwd)
if [ "$CI" '=' true ]
then
  export DATA_GCP_TEST_POSTGRES_PORT=5432
  export DB_NAME="db"
  export SQL_BASE_USER="postgres"
  export SQL_BASE="db"
  export SQL_CONNECTION_NAME="127.0.0.1"
else
  set +a; source ../../.env.local; set -a;
fi

[ "$CI" '!=' true ] && docker-compose up -d testdb
function wait_for_container () {(
    until PGPASSWORD=postgres psql -h localhost -p $DATA_GCP_TEST_POSTGRES_PORT -U "postgres" -c '\q'; do
      >&2 echo "Postgres is unavailable - sleeping"
      sleep 2
    done
)}
function run () {(
    pytest --cov
)}
sleep 3
wait_for_container
run
status=$?

[ "$CI" '!=' true ] && docker-compose stop testdb && docker-compose rm -f testdb

exit $status
