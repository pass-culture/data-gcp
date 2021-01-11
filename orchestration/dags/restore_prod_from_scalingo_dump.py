import datetime
import json
import os
import airflow
from airflow.operators import bash_operator
from airflow.contrib.operators.gcp_sql_operator import (
    CloudSqlInstanceImportOperator,
    CloudSqlQueryOperator,
)
from airflow.contrib.operators.gcs_acl_operator import (
    GoogleCloudStorageObjectCreateAclEntryOperator,
    GoogleCloudStorageBucketCreateAclEntryOperator,
)

# Global variables
GCS_BUCKET = "dump_scalingo_vm"
GCP_PROJECT_ID = "pass-culture-app-projet-test"
INSTANCE_NAME = os.environ.get("INSTANCE_APPLICATIVE_DATABASE", "dump-prod-8-10-2020")
DB_NAME = "test-restore-vm"

CLOUDSQL_APPLICATIVE_DATABASE = json.loads(
    os.environ.get("CLOUDSQL_APPLICATIVE_DATABASE_VM", "{}")
)
DESTINATION_DB_IP = CLOUDSQL_APPLICATIVE_DATABASE.get("ip", "")
DESTINATION_DB_SCHEMA = CLOUDSQL_APPLICATIVE_DATABASE.get("schema", "")
DESTINATION_DB_USER = CLOUDSQL_APPLICATIVE_DATABASE.get("user", "")
DESTINATION_DB_PASSWORD = CLOUDSQL_APPLICATIVE_DATABASE.get("password", "")
DESTINATION_DB_PORT = CLOUDSQL_APPLICATIVE_DATABASE.get("port", "")

DATABASE = CLOUDSQL_APPLICATIVE_DATABASE.get("schema")
INSTANCE_DATABASE = os.environ.get(
    "INSTANCE_APPLICATIVE_DATABASE", "dump-prod-8-10-2020"
)

LOCATION = "europe-west1"
TYPE = "postgres"

os.environ["AIRFLOW_CONN_POSTGRESQL_PROD_VM"] = (
    f"gcpcloudsql://{DESTINATION_DB_USER}:{DESTINATION_DB_PASSWORD}@{DESTINATION_DB_IP}:{DESTINATION_DB_PORT}/{DESTINATION_DB_SCHEMA}?"
    f"database_type={TYPE}&"
    f"project_id={GCP_PROJECT_ID}&"
    f"location={LOCATION}&"
    f"instance={INSTANCE_DATABASE}&"
    f"use_proxy=True&"
    f"sql_proxy_use_tcp=True"
)

now = datetime.datetime.now()
month = "{0:0=2d}".format(now.month)
day = "{0:0=2d}".format(now.day)
object_name = f"dump_{now.year}_{month}_{day}.sql"

default_args = {
    "retries": 1,
    "retry_delay": datetime.timedelta(minutes=5),
}

with airflow.DAG(
    "restore_prod_from_vm_export_v1",
    default_args=default_args,
    start_date=datetime.datetime(2020, 1, 10),
    description="Restore scalingo db after compute drop a dump on cloud storage",
    schedule_interval=None,
    catchup=False,
) as dag:
    sql_gcp_add_object_permission_task = GoogleCloudStorageObjectCreateAclEntryOperator(
        entity="user-p590556861198-y90rr9@gcp-sa-cloud-sql.iam.gserviceaccount.com",
        role="READER",
        bucket=GCS_BUCKET,
        object_name=object_name,
        task_id="sql_gcp_add_object_permission_task",
    )

    sql_gcp_add_bucket_permission_2_task = (
        GoogleCloudStorageBucketCreateAclEntryOperator(
            entity="user-p590556861198-y90rr9@gcp-sa-cloud-sql.iam.gserviceaccount.com",
            role="WRITER",
            bucket=GCS_BUCKET,
            task_id="sql_gcp_add_bucket_permission_2_task",
        )
    )

    clean_database_task = CloudSqlQueryOperator(
        gcp_cloudsql_conn_id="postgresql_prod_vm",
        task_id=f"drop_tables",
        sql=f"""
            DROP SCHEMA public CASCADE;
            CREATE SCHEMA public;
            CREATE EXTENSION postgis;
            CREATE EXTENSION unaccent;
            CREATE EXTENSION btree_gist;
        """,
        autocommit=True,
    )

    import_body = {
        "importContext": {
            "fileType": "sql",
            "uri": f"gs://{GCS_BUCKET}/{object_name}",
            "database": DB_NAME,
            "importUser": "postgres",
        }
    }

    sql_import_task = CloudSqlInstanceImportOperator(
        project_id=GCP_PROJECT_ID,
        body=import_body,
        instance=INSTANCE_NAME,
        task_id="sql_import_task",
    )

    sql_gcp_add_object_permission_task >> sql_gcp_add_bucket_permission_2_task >> clean_database_task >> sql_import_task