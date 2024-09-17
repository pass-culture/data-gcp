from dataclasses import dataclass

from common.config import (
    ENV_SHORT_NAME,
    GCE_BACKEND_NETWORK_ID,
    GCE_BACKEND_SUBNETWORK_ID,
    GCE_NETWORK_ID,
    GCE_SUBNETWORK_ID,
    GCE_VPC_SHARED_HOST_NETWORK_ID,
    GCE_VPC_SHARED_HOST_SUBNETWORK_ID,
)


@dataclass
class VPCNetwork:
    network_id: str = GCE_NETWORK_ID
    subnetwork_id: str = GCE_SUBNETWORK_ID


DEFAULT_VPC_NETWORK = VPCNetwork()
DATA_EHP_VPC_NETWORK = (
    VPCNetwork(
        network_id=GCE_BACKEND_NETWORK_ID,
        subnetwork_id=GCE_BACKEND_SUBNETWORK_ID,
    ),
)  # Allows to connect to vpc-data-ehp and comminucate with the backend services in ehp
HOST_EHP_VPC_NETWORK = VPCNetwork(
    network_id=GCE_VPC_SHARED_HOST_NETWORK_ID,
    subnetwork_id=GCE_VPC_SHARED_HOST_SUBNETWORK_ID,
)  # Allows to connect to vpc-host-ehp and communicate with the ClickHouse cluster in ehp


BASE_NETWORK_LIST: list[VPCNetwork] = (
    [DEFAULT_VPC_NETWORK, DATA_EHP_VPC_NETWORK]
    if ENV_SHORT_NAME != "prod"
    else [DEFAULT_VPC_NETWORK]
)
GKE_NETWORK_LIST: list[VPCNetwork] = (
    BASE_NETWORK_LIST + [HOST_EHP_VPC_NETWORK]
    if ENV_SHORT_NAME != "prod"
    else BASE_NETWORK_LIST
)

NETWORK_TYPE = {
    "GCE": BASE_NETWORK_LIST,
    "GKE": GKE_NETWORK_LIST,
}
