import subprocess


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
