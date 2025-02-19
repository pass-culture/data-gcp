#!/bin/bash
dbt compile --target='prod' --threads=16 --vars '{"ENV_SHORT_NAME": "prod"}'

dbt docs generate --static --target='prod' --threads=16 --vars '{"ENV_SHORT_NAME": "prod"}'
