import time
import logging
import csv
import gcsfs
from sh import pg_dump
from sshtunnel import SSHTunnelForwarder
from google.cloud import storage
from google.oauth2 import service_account
from datetime import datetime

credentials = service_account.Credentials.from_service_account_file(
    "/home/airflow/gcs/dags/credentials.json",
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)
client = storage.Client(
    credentials=credentials,
    project=credentials.project_id,
)

# ENV TESTING to transfer in env var with PC-5263
TESTING = {
    "port": 34469,
    "host": "pass-culture-407.postgresql.dbs.scalingo.com",
    "dbname": "pass_culture_407",
    "user": "pass_culture_407",
    "password": "szJm55of481Jfl2LS7W3",
}
LOCAL_HOST = "localhost"
LOCAL_PORT = "10000"

bucket_scalingo = "dump_scalingo"

# fs = gcsfs.GCSFileSystem(project='pass-culture-app-projet-test')
# file_exist = fs.exists(path)

# with fs.open(path) as file:
#     df = pd.read_csv(file, low_memory=False, **kwargs)
# return df

ssh_port = 22
ssh_username = "git"
ssh_hotname = "ssh.osc-fr1.scalingo.com"


def define_server():

    ## A REMPLIR
    return SSHTunnelForwarder(
        (ssh_hotname, ssh_port),
        ssh_username=ssh_username,
        ssh_pkey="/home/airflow/gcs/key_file.pem",
        local_bind_address=(LOCAL_HOST, LOCAL_PORT),
        remote_bind_address=(TESTING["host"], TESTING["port"]),
    )


def dump_scalingo():

    server = define_server()
    server.daemon_forward_servers = True

    server.start()

    try:

        dump_db()

    finally:
        server.stop()

    logging.info("Done.")
    return


def dump_db():

    # File path and name.
    file_path = f"gs://{bucket_scalingo}/raw/"
    now = datetime.now()
    file_name = f"{now.year}_{now.month}_{now.day}_scalingo.pgdump"

    # try:
    fs = gcsfs.GCSFileSystem(project="pass-culture-app-projet-test")

    # Running ssh command
    tunnel_database_url = (
        "postgres://{user}:{password}@{host}:{port}/{schema}?sslmode=prefer".format(
            user=TESTING["user"],
            password=TESTING["password"],
            host=LOCAL_HOST,
            port=LOCAL_PORT,
            schema=TESTING["dbname"],
        )
    )

    with fs.open(file_path + file_name, "w") as f:
        pg_dump(
            tunnel_database_url,
            "--exclude-table=spatial_ref_sys",
            "--exclude-table=recommendation",
            "--exclude-table=activity",
            "--exclude-table=email",
            "--exclude-table=user_session",
            _out=f,
        )

    # except Error as e:
