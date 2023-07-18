import os
import pandas as pd
from datetime import datetime
import time
import subprocess
import rii
import nanopq
import pickle
import numpy as np

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "passculture-data-ehp")
ENV_SHORT_NAME = os.environ.get("ENV_SHORT_NAME", "dev")
BIGQUERY_CLEAN_DATASET = f"clean_{ENV_SHORT_NAME}"
MODELS_RESULTS_TABLE_NAME = "mlflow_training_results"


def set_up_index(model_weights, path):
    size = model_weights.shape[0]
    train_vector = model_weights[np.random.choice(size, int(size * 0.3), replace=False)]
    codec = nanopq.PQ(M=32, Ks=256, verbose=True).fit(vecs=train_vector)
    index = rii.Rii(fine_quantizer=codec)
    index.add_configure(vecs=model_weights, nlist=None, iter=5)
    with open(path, "wb") as f:
        pickle.dump(index, f)


def save_experiment(experiment_name, model_name, serving_container, run_id):

    log_results = {
        "execution_date": datetime.now().isoformat(),
        "experiment_name": experiment_name,
        "model_name": model_name,
        "model_type": "custom",
        "run_id": run_id,
        "run_start_time": int(time.time() * 1000.0),
        "run_end_time": int(time.time() * 1000.0),
        "artifact_uri": None,
        "serving_container": serving_container,
    }
    pd.DataFrame.from_dict([log_results], orient="columns").to_gbq(
        f"""{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}""",
        project_id=f"{GCP_PROJECT_ID}",
        if_exists="append",
    )


def deploy_container(serving_container):
    command = f"sh ./deploy_to_docker_registery.sh {serving_container}"
    results = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )
    # TODO handle errors
    for line in results.stdout:
        print(line.rstrip().decode("utf-8"))


def get_items_metadata():
    sql = f"""
    SELECT distinct search_group_name as category, item_id from `{GCP_PROJECT_ID}.analytics_{ENV_SHORT_NAME}.recommendable_items_raw`
    """
    pd.read_gbq(sql).to_parquet("./metadata/item_metadata.parquet")
