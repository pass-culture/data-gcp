from dataclasses import dataclass

from common.config import (
    ENV_SHORT_NAME,
    GCE_NETWORK_ID,
    GCE_SUBNETWORK_ID,
    GCE_VPC_SHARED_HOST_NETWORK_ID,
    GCE_VPC_SHARED_HOST_SUBNETWORK_ID,
)


@dataclass
class DefaultVPCNetwork:
    network_id: str = GCE_NETWORK_ID
    subnetwork_id: str = GCE_SUBNETWORK_ID


DEFAULT_GKE_NETWORK = [DefaultVPCNetwork()]

# This is needed only for ehp environment.
if ENV_SHORT_NAME != "prod":
    DEFAULT_GKE_NETWORK.append(
        DefaultVPCNetwork(
            network_id=GCE_VPC_SHARED_HOST_NETWORK_ID,
            subnetwork_id=GCE_VPC_SHARED_HOST_SUBNETWORK_ID,
        )
    )
NETWORK_TYPE = {
    "GKE": DEFAULT_GKE_NETWORK,
    "GCE": [DefaultVPCNetwork()],
}
