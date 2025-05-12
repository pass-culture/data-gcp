from datetime import datetime

import typer

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
    model_name: str = typer.Option(
        "default",
        help="Name of the model",
    ),
    container_worker: str = typer.Option(
        "2",
        help="Number of workers",
    ),
    serving_container: str = typer.Option(
        ...,
        help="Serving container",
    ),
) -> None:
    run_id = f"{model_name}_{ENV_SHORT_NAME}_v{datetime.now().strftime('%Y%m%d')}"
    # serving_container = f"europe-west1-docker.pkg.dev/passculture-infra-prod/pass-culture-artifact-registry/data-gcp/retrieval-vector/{ENV_SHORT_NAME}/{experiment_name.replace('.', '_')}:{run_id}"

    deploy_container(serving_container, workers=int(container_worker))
    save_experiment(experiment_name, model_name, serving_container, run_id=run_id)


if __name__ == "__main__":
    typer.run(main)
