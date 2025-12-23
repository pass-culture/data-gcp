import sys
from datetime import datetime
import os
import subprocess
import typer
from loguru import logger

from constants import (
    GCP_PROJECT,
    ENV_SHORT_NAME,
    EXPERIMENT_NAME,
    SERVING_CONTAINER,
)
experiment_name=EXPERIMENT_NAME
base_serving_container_path=SERVING_CONTAINER
container_worker="1"
def deploy_container(serving_container, workers):
    """
    Deploy container to Docker registry.

    Args:
        serving_container (str): Container image to deploy
        workers (int): Number of workers for the container

    Raises:
        subprocess.CalledProcessError: If deployment fails
    """
    command = f"sh ./deploy_to_docker_registery.sh {serving_container} {workers}"
    try:
        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            check=True,
        )
        print(result.stdout)
    except subprocess.CalledProcessError as e:
        print(f"Command failed with return code {e.returncode}: {e.output}")
        raise

def main():
    serving_container = (
        f"{base_serving_container_path}/{experiment_name}"
    )

    try:
        logger.info(f"Deploying container: {serving_container}...")
        deploy_container(serving_container, workers=int(container_worker))
        logger.info(f"Container deployed: {serving_container}")

    except Exception as e:
        logger.error(f"Failed to deploy container or save experiment: {e}")
        sys.exit(1)


if __name__ == "__main__":
    typer.run(main)
