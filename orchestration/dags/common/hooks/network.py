from dataclasses import dataclass

from common.config import (
    ENV_SHORT_NAME,
    VPC_DATA_EHP_NETWORK_ID,
    VPC_DATA_EHP_SUBNETWORK_ID,
    VPC_DEFAULT_NETWORK_ID,
    VPC_DEFAULT_SUBNETWORK_ID,
    VPC_HOST_EHP_NETWORK_ID,
    VPC_HOST_EHP_SUBNETWORK_ID,
)


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
