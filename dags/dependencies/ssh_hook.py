# -*- coding: utf-8 -*-

import logging
import os
import shlex
import stat
import subprocess

from airflow.hooks.base_hook import BaseHook


class SSHTunnelHook(BaseHook):
    def __init__(self, identity_file="key_file.pem"):
        self.ssh_conn_id = "ssh_connection"
        self.identity_file = identity_file
        self.tunnel = None

        super().__init__(self.ssh_conn_id)

    def create_tunnel(
        self,
        local_port: str,
        database_host: str,
        database_port: int,
        key: str,
    ):
        """
        Creates a tunnel between two hosts. Like ssh -L <LOCAL_PORT>:host:<REMOTE_PORT>.
        Hard coded in for now. Down the line, it will pull from connections panel.

        """

        # Write the key to a file to change permissions
        # The container dies after the task executes, so don't have to
        # worry about closing/deleting it.
        with open(self.identity_file, "w") as key_file:
            key_file.write(key)

        logging.info(os.listdir())

        os.chmod(self.identity_file, stat.S_IRWXU)

        # Based on https://doc.scalingo.com/platform/databases/access#encrypted-tunnel

        ssh_tunnel_command = """ssh -L {local_port}:{database_host}:{database_port} -i {identity_file} git@{ssh_hostname} -N""".format(
            local_port=local_port,
            database_host=database_host,
            database_port=database_port,
            identity_file=self.identity_file,
            ssh_hostname="ssh.osc-fr1.scalingo.com",
        )

        logging.info(
            "Opening tunnel with command : {ssh_tunnel_command}".format(
                ssh_tunnel_command=ssh_tunnel_command
            )
        )

        # Running ssh command
        args = shlex.split(ssh_tunnel_command)
        logging.info("Creating tunnel")
        self.tunnel = subprocess.Popen(args)

    def kill_tunnel(self):
        if not self.tunnel:
            logging.info("No existing tunnel")
        else:
            logging.info("Terminating tunnel")
            self.tunnel.kill()
