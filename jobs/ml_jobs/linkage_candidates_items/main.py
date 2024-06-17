import typer
from utils import read_parquet, upload_parquet

import pandas as pd
import json
from lancedb.pydantic import Vector, LanceModel
from loguru import logger
from semantic_space_classes import TextClient
import subprocess
import typer
BIGQUERY_CLEAN_DATASET="clean_prod"
LOCAL_RETRIEVAL_PATH='./retrieval_vector'
MODELS_RESULTS_TABLE_NAME = "mlflow_training_results"

app = typer.Typer()

class Item(LanceModel):
    """
    A Pydantic model representing an item with a vector, item_id, and performer.
    """
    vector: Vector(384)
    item_id: str
    performer: str

def load_model(retrieval_path) -> TextClient:
    """
    Load a TextClient model from a given path.

    Parameters:
    retrieval_path (str): Path to the model.

    Returns:
    TextClient: The loaded model.
    """
    with open(f"{retrieval_path}/metadata/model_type.json", "r") as file:
        desc = json.load(file)
        model=TextClient(transformer=desc["transformer"])
    model.load()
    return model

    

def get_semantic_candidates(model, vector, params, metric, k=40):
    """
    Get semantic candidates for a given vector using a model.

    Parameters:
    model (TextClient): The model to use for retrieval.
    vector (Vector): The vector to retrieve similar items for.
    params (dict): Additional parameters for the search.
    metric (str): The similarity metric to use.
    k (int): The number of results to return.

    Returns:
    DataFrame: A dataframe of the top k similar items.
    """
    return model.search(vector=vector, similarity_metric=metric, n=k, query_filter=params, details=False, item_id=None, prefilter=True, vector_column_name="vector")

def update_dataframe(df, item_id):
    """
    Update a dataframe with a new source_item_id and batch_id, and rename some columns.

    Parameters:
    df (DataFrame): The dataframe to update.
    item_id (str): The new source_item_id.

    Returns:
    DataFrame: The updated dataframe.
    """
    df["source_item_id"] = item_id
    df["batch_id"] = str(uuid.uuid4())[:5]
    df.rename(columns={"item_id":"link_item_id","source_item_id":"item_id"}, inplace=True)
    return df

def get_vector(model,input_text):
    """
    Get the vector representation of the input text using the model_retrieval model.

    Parameters:
    input_text (str): The input text.

    Returns:
    Vector: The vector representation of the input text.
    """
    return model.text_vector(input_text)

def preprocess_data(data):
    """
    Preprocess the given dataframe by filling NaN values in the 'performer' column,
    converting the 'offer_name' column to lowercase, and mapping the 'offer_name' 
    column to vectors.

    Parameters:
    data (DataFrame): The input dataframe.

    Returns:
    DataFrame: The preprocessed dataframe.
    """
    data['performer'] = data['performer'].fillna(value='unkn')
    data["offer_name"] = data["offer_name"].str.lower()
    data["embedding"] = data["embedding"].fillna(value="no_emb")
    return data

def prepare_vectors(model,data):
    data_w_emb = data.loc[data["embedding"] != "no_emb"]
    data_wo_emb = data.loc[data["embedding"] == "no_emb"]
    data_w_emb["vector"] = data_w_emb['embedding']
    data_wo_emb["vector"] = data_wo_emb['offer_name'].map(lambda x: get_vector(model,str(x))).tolist()
    return pd.concat([data_w_emb, data_wo_emb])

def download_model(experiment_name,run_id,local_retrieval_path):
    # if run_id is None or len(run_id) <= 2:
    #     results_array = pd.read_gbq(
    #         f"""SELECT * FROM `{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}` WHERE experiment_name = '{experiment_name}' ORDER BY execution_date DESC LIMIT 1"""
    #     ).to_dict("records")
    # else:
    #     results_array = pd.read_gbq(
    #         f"""SELECT * FROM `{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}` WHERE experiment_name = '{experiment_name}' AND run_id = '{run_id}' ORDER BY execution_date DESC LIMIT 1"""
    #     ).to_dict("records")

    # if len(results_array) == 0:
    #         raise Exception(
    #             f"Model {experiment_name} not found into BQ {MODELS_RESULTS_TABLE_NAME}. Failing."
    #         )
    # else:
    #     serving_container = results_array[0]["serving_container"]

    serving_container="eu.gcr.io/passculture-data-prod/retrieval_semantic_vector_v0_1_prod:v1.1_prod_prod_v20240618"
    
    # Download the Docker image from GCS
    subprocess.run(f"docker pull {serving_container}", shell=True, check=True)

    # Fetch the container ID
    container_id = subprocess.run("docker ps -lq", shell=True, capture_output=True, text=True).stdout.strip()

    # Copy files from the Docker container
    subprocess.run(f"docker cp {container_id}:/app/ {local_retrieval_path}", shell=True, check=True)

def generate_semanctic_candidates(
        data,
        experiment_name,
        run_id,
        ):
    """
    Load a local retrieval model, preprocess the data, and generate a linkage dataframe.

    This function loads a local retrieval model, reads a parquet file into a dataframe,
    and iterates over the rows of the dataframe. For each row, it gets the semantic candidates,
    updates the dataframe, and appends it to the linkage list. Finally, it concatenates the linkage
    list into a dataframe and writes it to a parquet file.

    Returns:
    None
    """
   
    download_model(experiment_name,run_id,LOCAL_RETRIEVAL_PATH)
    model_retrieval=load_model(LOCAL_RETRIEVAL_PATH)
    

    data_clean=preprocess_data(data)
    data_ready=prepare_vectors(model_retrieval,data_clean)
    
    linkage=[]
    for index, row in data_ready.iterrows():
        if index%100==0:
            logger.info(f"Processing {index}...")
        params={}
        result_df=get_semantic_candidates(model_retrieval, row.vector, params, 'cosine', k=20)
        if len(result_df)==0:
            logger.info("!! Warning No candidates !!")
            continue
        result_df = update_dataframe(result_df, row.item_id)
        linkage.append(result_df)

    df_linkage=pd.concat(linkage)
    return df_linkage

@app.command()
def main(
    source_file_path: str = typer.Option(),
    output_file_path: str = typer.Option(),
    experiment_name: str = typer.Option(None, help="model name to load ") ,
    run_id: str = typer.Option(None, help="run_id of the model"),
) -> None:
    
    data = read_parquet(source_file_path)
    # Your code here
    output_df = generate_semanctic_candidates(data,experiment_name,run_id)

    upload_parquet(
        dataframe=output_df,
        gcs_path=output_file_path,
    )


if __name__ == "__main__":
    app()




