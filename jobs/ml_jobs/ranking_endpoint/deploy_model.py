import pandas as pd
from datetime import datetime
import typer
from app.model import TrainPipeline
from utils import (
    GCP_PROJECT_ID,
    ENV_SHORT_NAME,
    deploy_container,
    save_experiment,
)

PARAMS = {"seen": 2_000_000, "consult": 2_000_000, "booking": 2_000_000}


def load_data(table_name):
    sql = f"""
    WITH seen AS (
      SELECT
          * 
      FROM `{GCP_PROJECT_ID}.{table_name}` 
      WHERE seen = True
      ORDER BY RAND()
      LIMIT {PARAMS['seen']}
    ),
    consult AS (
        SELECT
            * 
        FROM `{GCP_PROJECT_ID}.{table_name}` 
        WHERE consult = True
        ORDER BY RAND()
        LIMIT {PARAMS['consult']}

    ),
    booking AS (
      SELECT
            * 
        FROM `{GCP_PROJECT_ID}.{table_name}` 
        WHERE booking = True
        ORDER BY RAND()
        LIMIT {PARAMS['booking']}
    )
    
    SELECT * FROM seen 
    UNION ALL 
    SELECT * FROM consult 
    UNION ALL 
    select * FROM booking 
    """
    return pd.read_gbq(sql).sample(frac=1)


def train_pipeline(table_name):
    data = load_data(table_name)
    pipeline = TrainPipeline()
    pipeline.set_pipeline()
    pipeline.train(data)
    pipeline.save()


def main(
    experiment_name: str = typer.Option(
        None,
        help="Name of the experiment",
    ),
    model_name: str = typer.Option(
        None,
        help="Name of the model",
    ),
    input_table: str = typer.Option(
        None,
        help="Name input table with preproc data",
    ),
) -> None:

    yyyymmdd = datetime.now().strftime("%Y%m%d")
    if model_name is None:
        model_name = "default"
    run_id = f"{model_name}_{ENV_SHORT_NAME}_v{yyyymmdd}"
    serving_container = (
        f"eu.gcr.io/{GCP_PROJECT_ID}/{experiment_name.replace('.', '_')}:{run_id}"
    )
    train_pipeline(table_name=input_table)
    print("Deploy...")
    deploy_container(serving_container)
    save_experiment(experiment_name, model_name, serving_container, run_id=run_id)


if __name__ == "__main__":
    typer.run(main)
