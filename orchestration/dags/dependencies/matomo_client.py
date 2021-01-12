from airflow.contrib.hooks.ssh_hook import SSHHook
from airflow.hooks.mysql_hook import MySqlHook


class MatomoClient:
    def __init__(self, matomo_connection_data, local_port):
        self.matomo_connection_data = matomo_connection_data
        self.local_port = local_port

    def create_tunnel(self):
        ssh_hook = SSHHook(
            ssh_conn_id="ssh_scalingo",
            keepalive_interval=120,
        )
        tunnel = ssh_hook.get_tunnel(
            remote_port=self.matomo_connection_data.get("port", 0),
            remote_host=self.matomo_connection_data.get("host", 0),
            local_port=self.local_port,
        )
        return tunnel

    def query(self, query_string):
        tunnel = self.create_tunnel()
        tunnel.start()
        mysql_hook = MySqlHook(mysql_conn_id="mysql_scalingo")
        result = mysql_hook.get_records(sql=query_string)
        tunnel.stop()
        return result
