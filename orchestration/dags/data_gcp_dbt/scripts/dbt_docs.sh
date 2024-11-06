#!/bin/bash
dbt docs generate --target='prod'

dbt docs serve
