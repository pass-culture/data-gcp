from google.cloud import aiplatform
from dataclasses import dataclass
import typer
from utils import (
    GCP_PROJECT_ID,
    BIGQUERY_CLEAN_DATASET,
    MODELS_RESULTS_TABLE_NAME,
    ENV_SHORT_NAME,
)
import pandas as pd


@dataclass
class TFContainer:
    serving_container: str
    artifact_uri: str = None
    serving_container_predict_route = None
    serving_container_health_route = None
    serving_container_ports = None


@dataclass
class CustomContainer(TFContainer):
    serving_container_predict_route = "/predict"
    serving_container_health_route = "/isalive"
    serving_container_ports = [8080]


@dataclass
class ModelParams:
    experiment_name: str
    version_name: str
    model_description: str
    container_type: TFContainer


@dataclass
class EndpointParams:
    endpoint_name: str
    min_nodes: int
    max_nodes: int
    machine_type: str = "n1-standard-2"
    traffic_percentage: int = 100


class ModelHandler:
    def __init__(
        self,
        region: str,
        project_name: str,
        model_params: ModelParams,
        endpoint_params: EndpointParams,
    ) -> None:
        self.region = region
        self.project_name = project_name
        self.model_params = model_params
        self.endpoint_params = endpoint_params

    def upload_model(self):
        experiment_name = self.model_params.experiment_name
        print(f"Uploading model to Vertex AI model registery...")
        print(f"Search for existing model...  {experiment_name}")
        parent_model = aiplatform.Model.list(
            filter=f"display_name={experiment_name}",
            location=self.region,
            project=self.project_name,
        )
        # case no parent, then create a new one.
        if len(parent_model) > 0:
            print("Parent found, deploying new version")
            parent_model_id = parent_model[0].name
            version_name = f"{experiment_name}_{self.model_params.version_name}"
        else:
            print("Parent model not found, deploying new version..")
            parent_model_id = None
            version_name = experiment_name

        model = aiplatform.Model.upload(
            display_name=version_name,
            parent_model=parent_model_id,
            project=self.project_name,
            location="europe-west1",
            artifact_uri=self.model_params.container_type.artifact_uri,
            serving_container_image_uri=self.model_params.container_type.serving_container,
            description=self.model_params.model_description,
            serving_container_predict_route=self.model_params.container_type.serving_container_predict_route,
            serving_container_health_route=self.model_params.container_type.serving_container_health_route,
            serving_container_ports=self.model_params.container_type.serving_container_ports,
        )
        return model

    def deploy_model(self, model):
        enpoint_name = self.endpoint_params.endpoint_name
        print(f"Deploying model to endpoint...")
        print(f"Search for existing endoipoint...  {enpoint_name}")
        found_endpoints = aiplatform.Endpoint.list(
            filter=f"display_name={enpoint_name}",
            location=self.region,
            project=self.project_name,
        )
        if len(found_endpoints) == 0:
            print("Endpoint not found, deploying new one..")
            endpoint = aiplatform.Endpoint.create(
                display_name=enpoint_name,
                project=self.project_name,
                location=self.region,
            )
        else:
            endpoint = found_endpoints[0]

        version_name = (
            f"{self.model_params.experiment_name}_{self.model_params.version_name}"
        )
        print(f"Deploy model version {version_name} to endpoint {enpoint_name}...")
        model.deploy(
            endpoint=endpoint,
            deployed_model_display_name=version_name,
            min_replica_count=self.endpoint_params.min_nodes,
            max_replica_count=self.endpoint_params.max_nodes,
            machine_type=self.endpoint_params.machine_type,
            traffic_percentage=self.endpoint_params.traffic_percentage,
        )
        model.wait()

        print("Undeploy old versions..")
        endpoint_dict = endpoint.to_dict()
        deployed_models_sorted_by_date = sorted(
            endpoint_dict["deployedModels"], key=lambda d: d["createTime"], reverse=True
        )
        # Undeploy oldies
        if len(deployed_models_sorted_by_date) > 1:
            print(f"Found {len(deployed_models_sorted_by_date)}")
            for previous_model in deployed_models_sorted_by_date[1:]:
                previous_version_model_id = previous_model["id"]
                print(f"Undeploying id : {previous_version_model_id}")
                endpoint.undeploy(previous_version_model_id)

    def clean_model_versions(self, max_model_versions):
        model = aiplatform.Model.list(
            filter=f"display_name={self.model_params.experiment_name}",
            location=self.region,
            project=self.project_name,
        )

        if len(model) > 0:
            print(f"Found {len(model)} model")
            model_id = model[0].name

            model = aiplatform.Model(
                model_name=f"projects/{self.project_name}/locations/{self.region}/models/{model_id}"
            )
            modelRegistry = aiplatform.models.ModelRegistry(
                model,
                self.region,
                self.project_name,
            )

            versions = modelRegistry.list_versions()
            if len(versions) < max_model_versions:
                print(f"Only {len(versions)}, pass.")
            else:
                versions_to_clean = versions[:-max_model_versions]
                for versions in versions_to_clean:
                    try:
                        print(f"Removing {versions.version_id}")
                        modelRegistry.delete_version(f"{versions.version_id}")
                    except:
                        # TODO; Model might be used by another endpoint
                        # Check if deployed or not before.
                        print(f"Could not remove {versions.version_id}")
                        pass


def main(
    region: str = typer.Option(None, help="Region to deploy"),
    experiment_name: str = typer.Option(None, help="Experiment of the Model"),
    endpoint_name: str = typer.Option(None, help="Endpoint of the Model"),
    version_name: str = typer.Option(None, help="Version of the Model"),
    model_description: str = typer.Option(None, help="Description of the Model"),
    artifact_uri: str = typer.Option(None, help="Path of the model in gcs"),
    serving_container: str = typer.Option(None, help="docker base container"),
    model_type: str = typer.Option(
        "tensorflow",
        help="Model Type (tenforflow|custom)",
    ),
    machine_type: str = typer.Option(
        "n1-standard-2",
        help="Machine type to deploy",
    ),
    traffic_percentage=typer.Option(
        100,
        help="The traffic percentage to deploy to the endpoint",
    ),
    min_nodes=typer.Option(
        1,
        help="Total min nodes to deploy",
    ),
    max_nodes=typer.Option(
        5,
        help="Total max nodes to deploy",
    ),
) -> None:
    MODEL_TYPE_CONFIG = {"tensorflow": TFContainer, "custom": CustomContainer}
    # Load model stats from BQ
    if artifact_uri is None or serving_container is None:
        results_array = pd.read_gbq(
            f"""SELECT * FROM `{BIGQUERY_CLEAN_DATASET}.{MODELS_RESULTS_TABLE_NAME}` WHERE experiment_name = '{experiment_name}' ORDER BY execution_date DESC LIMIT 1"""
        ).to_dict("records")
        if len(results_array) == 0:
            raise Exception(
                f"Model {experiment_name} not found into BQ {MODELS_RESULTS_TABLE_NAME}. Failing."
            )
        else:
            artifact_uri = results_array[0]["artifact_uri"]
            serving_container = results_array[0]["serving_container"]
            model_type = results_array[0]["model_type"]
            # fallback to default description.
            if model_description is None:
                model_description = f"""{model_type} {experiment_name}."""

    container_type = MODEL_TYPE_CONFIG[model_type](
        serving_container=serving_container, artifact_uri=artifact_uri
    )
    model_params = ModelParams(
        experiment_name.replace(".", "_"),
        version_name,
        model_description,
        container_type,
    )

    endpoint_params = EndpointParams(
        endpoint_name=endpoint_name,
        min_nodes=int(min_nodes),
        max_nodes=int(max_nodes),
        machine_type=machine_type,
        traffic_percentage=int(traffic_percentage),
    )
    handler = ModelHandler(region, GCP_PROJECT_ID, model_params, endpoint_params)
    # Upload new model to registery
    model = handler.upload_model()
    # Deploy it to an endpoint
    handler.deploy_model(model)
    # Delete old model versions
    max_model_versions = 5 if ENV_SHORT_NAME == "prod" else 1
    handler.clean_model_versions(max_model_versions)


if __name__ == "__main__":
    typer.run(main)
