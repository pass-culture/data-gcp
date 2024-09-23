from dataclasses import dataclass

from common.config import ENV_SHORT_NAME, GCP_PROJECT_ID, GCP_REGION

VPC_DEFAULT_NETWORK_ID_PER_ENV = {
    "dev": f"projects/{GCP_PROJECT_ID}/global/networks/vpc-data-dev",
    "stg": f"projects/{GCP_PROJECT_ID}/global/networks/vpc-data-staging",
    "prod": f"projects/{GCP_PROJECT_ID}/global/networks/vpc-data-prod",
}
VPC_DEFAULT_SUBNETWORK_ID_PER_ENV = {
    "dev": f"projects/{GCP_PROJECT_ID}/regions/{GCP_REGION}/subnetworks/data-dev-private",
    "stg": f"projects/{GCP_PROJECT_ID}/regions/{GCP_REGION}/subnetworks/data-staging-private",
    "prod": f"projects/{GCP_PROJECT_ID}/regions/{GCP_REGION}/subnetworks/data-prod-private",
}
VPC_DEFAULT_NETWORK_ID = VPC_DEFAULT_NETWORK_ID_PER_ENV[ENV_SHORT_NAME]
VPC_DEFAULT_SUBNETWORK_ID = VPC_DEFAULT_SUBNETWORK_ID_PER_ENV[ENV_SHORT_NAME]
# Allows to communicate with API reco and ClickHouse
VPC_DATA_EHP_NETWORK_ID = "projects/passculture-data-ehp/global/networks/vpc-data-ehp"
VPC_DATA_EHP_SUBNETWORK_ID = "projects/passculture-data-ehp/regions/europe-west1/subnetworks/cloudruns-ilb-subnet-ehp"
VPC_HOST_EHP_NETWORK_ID = "projects/passculture-host-ehp/global/networks/vpc-host-ehp"
VPC_HOST_EHP_SUBNETWORK_ID = "projects/passculture-host-ehp/regions/europe-west1/subnetworks/passculture-data-ehp"


@dataclass
class VPCNetwork:
    network_id: str = VPC_DEFAULT_NETWORK_ID
    subnetwork_id: str = VPC_DEFAULT_SUBNETWORK_ID


DEFAULT_VPC_NETWORK = VPCNetwork()
DATA_EHP_VPC_NETWORK = VPCNetwork(
    network_id=VPC_DATA_EHP_NETWORK_ID, subnetwork_id=VPC_DATA_EHP_SUBNETWORK_ID
)  # Allows to connect to vpc-data-ehp and communicate with services inside vpc-data-ehp, like api-reco
HOST_EHP_VPC_NETWORK = VPCNetwork(
    network_id=VPC_HOST_EHP_NETWORK_ID,
    subnetwork_id=VPC_HOST_EHP_SUBNETWORK_ID,
)  # Allows to connect to vpc-host-ehp and communicate with services inside, like Clickhouse

BASE_NETWORK_LIST: list[VPCNetwork] = (
    [DEFAULT_VPC_NETWORK, DATA_EHP_VPC_NETWORK]
    if ENV_SHORT_NAME != "prod"
    else [DEFAULT_VPC_NETWORK]
)
GKE_NETWORK_LIST: list[VPCNetwork] = (
    [DEFAULT_VPC_NETWORK, DATA_EHP_VPC_NETWORK, HOST_EHP_VPC_NETWORK]
    if ENV_SHORT_NAME != "prod"
    else [DEFAULT_VPC_NETWORK]
)
