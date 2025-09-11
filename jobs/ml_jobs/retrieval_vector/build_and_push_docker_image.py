import sys
from datetime import datetime

import typer
from loguru import logger

from utils import (
    ENV_SHORT_NAME,
    deploy_container,
    save_experiment,
)


def main(
    experiment_name: str = typer.Option(
        ...,
        help="Name of the experiment",
    ),
    base_serving_container_path: str = typer.Option(
        ...,
        help="Base path of the serving container",
    ),
    model_name: str = typer.Option(
        "default",
        help="Name of the model",
    ),
    container_worker: str = typer.Option(
        "2",
        help="Number of workers",
    ),
) -> None:
    run_id = f"{model_name}_{ENV_SHORT_NAME}_v{datetime.now().strftime('%Y%m%d')}"
    serving_container = (
        f"{base_serving_container_path}/{experiment_name.replace('.', '_')}:{run_id}"
    )

    try:
        logger.info(f"Deploying container: {serving_container}...")
        deploy_container(serving_container, workers=int(container_worker))
        logger.info(f"Container deployed: {serving_container}")

        logger.info(f"Saving experiment: {experiment_name} in MLFlow...")
        save_experiment(experiment_name, model_name, serving_container, run_id=run_id)
        logger.info(
            f"Experiment saved: {experiment_name} in MLFlow with run_id: {run_id}"
        )

    except Exception as e:
        logger.error(f"Failed to deploy container or save experiment: {e}")
        sys.exit(1)


if __name__ == "__main__":
    typer.run(main)
