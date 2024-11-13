#!/bin/bash
dbt compile

dbt docs generate --target='prod'

dbt docs serve
