#!/bin/bash

export PYTHONPATH=$PYTHONPATH:$(pwd)
if [ "$CI" '=' true ]
then
  export DATA_GCP_TEST_POSTGRES_PORT=5432
else
  set +a; source ../../.env.local; set -a;
fi

[ "$CI" '!=' true ] && docker-compose up -d testdb
function wait_for_container () {(
    until PGPASSWORD=postgres psql -h localhost -p $DATA_GCP_TEST_POSTGRES_PORT -U "postgres" -c '\q'; do
      >&2 echo "Postgres is unavailable - sleeping"
      sleep 1
    done
)}
function run () {(
  pytest tests -s
)}
wait_for_container
run
status=$?

[ "$CI" '!=' true ] && docker-compose stop testdb && docker-compose rm -f testdb

# exit $status
