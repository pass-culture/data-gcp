#!/bin/bash

if [ "$CI" '!=' true ]
then
  export PORT=5433
else
  export PORT=5432
fi

[ "$CI" '!=' true ] && docker-compose up -d testdb
function run () {(
    set -e
    export ENVIRONMENT=test
    until PGPASSWORD=postgres psql -h localhost -p $PORT -U "postgres" -c '\q'; do
      >&2 echo "Postgres is unavailable - sleeping"
      sleep 1
    done
    poetry run pytest tests/test_recommendable_offers.py
)}
run
status=$?

[ "$CI" '!=' true ] && docker-compose stop testdb && docker-compose rm -f testdb

exit $status
