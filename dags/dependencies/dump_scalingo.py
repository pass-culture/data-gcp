import time
import logging
import csv
import psycopg2
from sshtunnel import SSHTunnelForwarder
from dependencies.ssh_hook import SSHTunnelHook

ENV = {
    "port": 0,
    "host": "",
    "dbname": "",
    "user": "",
    "password": "",
}
LOCAL_HOST = "localhost"
LOCAL_PORT = "10000"
PRIVATE_SSH_KEY = """"""


def define_server():

    ## A REMPLIR
    return SSHTunnelForwarder(
        (ssh_host, ssh_port),
        ssh_username=self.ssh_user_name,
        ssh_pkey="key_file.pem",
        local_bind_address=("localhost", "10000"),
        remote_bind_address=(self.ssh_remote_bind_host, self.ssh_remote_bind_port),
    )


def dump_scalingo():

    server = define_server()
    server.daemon_forward_servers = True

    server.start()

    try:

        dump_db()

    finally:
        hook.kill_tunnel()

    logging.info("Done.")
    return


def dump_db():

    # File path and name.
    file_path = "/home/airflow/gcs/data"
    file_name = "test.csv"

    # Database connection variable.
    connect = None

    try:

        # Connect to database.
        connect = psycopg2.connect(
            host=LOCAL_HOST,
            database=ENV["dbname"],
            user=ENV["user"],
            password=ENV["password"],
            port=LOCAL_PORT,
        )

    except psycopg2.DatabaseError:

        # Confirm unsuccessful connection and stop program execution.
        logging.info("Database connection unsuccessful.")
        quit()

    # Cursor to execute query.
    cursor = connect.cursor()

    # SQL to select data from the person table.
    sql_select = "SELECT * from offer"

    try:

        # Execute query.
        cursor.execute(sql_select)

        # Fetch the data returned.
        results = cursor.fetchall()

        # Extract the table headers.
        headers = [i[0] for i in cursor.description]

        # Open CSV file for writing.
        csv_file = csv.writer(
            open(file_path + file_name, "w", newline=""),
            delimiter=",",
            lineterminator="\r\n",
            quoting=csv.QUOTE_ALL,
            escapechar="\\",
        )

        # Add the headers and data to the CSV file.
        csv_file.writerow(headers)
        csv_file.writerows(results)

        # Message stating export successful.
        logging.info("Data export successful.")

    except psycopg2.DatabaseError:

        # Message stating export unsuccessful.
        logging.info("Data export unsuccessful.")

    finally:

        # Close database connection.
        connect.close()
