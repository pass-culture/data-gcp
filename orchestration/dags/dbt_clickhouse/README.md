# dbt_clickhouse

## Overview

This project is a dbt (data build tool) setup for ClickHouse, designed to manage and transform analyticals data for our partners in clickhouse using dbt's capabilities.

## Prerequisites

1. Install uv (see data gcp repo if not installed yet)

2. Install kubernetes tools:

    kubectl -> <https://kubernetes.io/fr/docs/tasks/tools/install-kubectl/>

    kubens -> <https://webinstall.dev/kubens/>

3. Install pc-connect to acces K8s clusters:

    download & install this other repo (not in the same folder please)

    <https://github.com/pass-culture/pc-connect>

## Installation

1. Initialization

    run `make init`

2. Database Secrets

    Uncomment and set clickhouse credentials in .env.local
Secrets canbe found using google cloud secret manager (gcpadmin account, project passculture-data-ehp)
<https://console.cloud.google.com/security/secret-manager?hl=fr&inv=1&invt=AbxdCg&project=passculture-data-ehp>

    - user -> data-development_clickhouse_user
    - password -> data-development_clickhouse_password

3. Setup environement

    run `make setup`

## Usage

1. Tunnel to the cluster & connect to the database
Before you can run any dbt commands locally you must tunnel to the cluster.

you must be authenticated first (`gcloud auth application-default login` gcpadmin)

then run `make connect_clickhouse`

you should now be connected clickhouse

2. Test your connection to the database:

- activate your virtualenv as prompted
- run `dbt debug` (green is good, red is bad, sorry colorblinds you'll have to actually read the logs)
- If succesful you can enjoy regular dbt cli (custom command such as --defer-to are not available)

3. Beware! step 1 perform a port-forward running in background it is a vulnerability

Once you are done working on clickhouse Make sure to close the connection

run `make disconnect_clickhouse`
