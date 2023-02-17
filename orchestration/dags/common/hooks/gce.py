from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import pytz, json, os, time
import dateutil
import datetime
from dataclasses import dataclass
from common.config import (
    ENV_SHORT_NAME,
    GCP_REGION,
    GCE_ZONE,
    GCP_PROJECT_ID,
    GCE_SUBNETWORK_ID,
    GCE_NETWORK_ID,
    GCE_SA,
)

DEFAULT_LABELS = {"env": ENV_SHORT_NAME, "terraform": "false", "airflow": "true"}


@dataclass
class CPUImage:
    source_image: str = "projects/deeplearning-platform-release/global/images/tf2-latest-cpu-v20211202"  # tf-latest-cpu-v20221219
    startup_script: str = None
    startup_script_wait_time: int = 0


@dataclass
class GPUImage:
    source_image: str = "projects/deeplearning-platform-release/global/images/tf2-latest-gpu-v20211202"  # tf-latest-gpu-v20221219
    startup_script: str = """
        #!/bin/bash
        sudo /opt/deeplearning/install-driver.sh
    """
    startup_script_wait_time: int = 90


class GCEHook(GoogleBaseHook):
    _conn = None

    def __init__(
        self,
        gcp_project=GCP_PROJECT_ID,
        gcp_zone=GCE_ZONE,
        gcp_region=GCP_REGION,
        gce_network_id=GCE_NETWORK_ID,
        gce_subnetwork_id=GCE_SUBNETWORK_ID,
        gce_sa=GCE_SA,
        source_image_type=CPUImage(),
        gcp_conn_id: str = "google_cloud_default",
        delegate_to: str = None,
        impersonation_chain: str = None,
    ):
        self.gcp_project = gcp_project
        self.gcp_zone = gcp_zone
        self.gcp_region = gcp_region
        self.gce_network_id = gce_network_id
        self.gce_subnetwork_id = gce_subnetwork_id
        self.gce_sa = gce_sa
        self.source_image_type = source_image_type
        super().__init__(
            gcp_conn_id=gcp_conn_id,
            delegate_to=delegate_to,
            impersonation_chain=impersonation_chain,
        )

    def get_conn(self):
        if self._conn is None:
            self._conn = googleapiclient.discovery.build(
                "compute",
                "v1",
                cache_discovery=False,
            )
        return self._conn

    def start_vm(
        self,
        instance_name,
        instance_type,
        preemptible,
        accelerator_types=[],
    ):
        instances = self.list_instances()
        instances = [x["name"] for x in instances if x["status"] == "RUNNING"]
        if instance_name in instances:
            self.log.info(f"Instance {instance_name} already running, pass.")
            return

        self.log.info(
            f"Launching {instance_name} on compute engine (instance: {instance_type})"
        )
        self.__create_instance(
            instance_type,
            instance_name,
            wait=True,
            preemptible=preemptible,
            accelerator_types=accelerator_types,
        )

    def delete_vm(self, instance_name):
        self.log.info(f"Removing {instance_name} on compute engine")
        self.__delete_instance(instance_name, wait=True)

    def list_instances(self, filter=None):
        result = (
            self.get_conn()
            .instances()
            .list(project=self.gcp_project, zone=self.gcp_zone, filter=filter)
            .execute()
        )
        return result.get("items", [])

    def get_instance(self, name):
        try:
            return (
                self.get_conn()
                .instances()
                .get(instance=name, project=self.gcp_project, zone=self.gcp_zone)
                .execute()
            )
        except HttpError as e:
            if e.resp.status == 404:
                return None
            else:
                raise

    def __create_instance(
        self,
        instance_type,
        name,
        metadata=None,
        wait=False,
        accelerator_types=[],
        preemptible=False,
    ):
        instance_type = "zones/%s/machineTypes/%s" % (self.gcp_zone, instance_type)
        accelerator_type = [
            {
                "acceleratorCount": [a_t["count"]],
                "acceleratorType": "zones/%s/acceleratorTypes/%s"
                % (self.gcp_zone, a_t["name"]),
            }
            for a_t in accelerator_types
        ]
        metadata = (
            [{"key": key, "value": value} for key, value in metadata.items()]
            if metadata
            else []
        )
        if self.source_image_type.startup_script is not None:
            metadata = metadata + [
                {
                    "key": "startup-script",
                    "value": self.source_image_type.startup_script,
                }
            ]

        config = {
            "name": name,
            "machineType": instance_type,
            # Specify the boot disk and the image to use as a source.
            "disks": [
                {
                    "boot": True,
                    "autoDelete": True,
                    "initialize_params": {
                        "disk_size_gb": "50",
                        "sourceImage": self.source_image_type.source_image,
                    },
                }
            ],
            # Specify VPC network interface
            "networkInterfaces": [
                {"network": self.gce_network_id, "subnetwork": self.gce_subnetwork_id}
            ],
            "serviceAccounts": [
                {
                    "email": f"{self.gce_sa}@{self.gcp_project}.iam.gserviceaccount.com",
                    "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
                }
            ],
            "metadata": {"items": metadata},
            "tags": {"items": ["training"]},
            "labels": DEFAULT_LABELS,
        }
        # GPUs
        if len(accelerator_type) > 0:
            config["guestAccelerators"] = accelerator_type
        if preemptible:
            config["scheduling"] = {
                "onHostMaintenance": "terminate",
                "preemptible": True,
            }
        else:
            config["scheduling"] = {"onHostMaintenance": "terminate"}

        self.log.info(
            f"Creating {name}: \n {json.dumps(config, sort_keys=True, indent=4)}"
        )
        operation = (
            self.get_conn()
            .instances()
            .insert(project=self.gcp_project, zone=self.gcp_zone, body=config)
            .execute()
        )

        if wait:
            # force some waiting time for startup script.
            if self.source_image_type.startup_script_wait_time > 0:
                time.sleep(self.source_image_type.startup_script_wait_time)
            self.wait_for_operation(operation["name"])
        else:
            return operation["name"]

    def __delete_instance(self, name, wait=False):
        self.log.info(f"Deleting {name}")
        try:
            operation = (
                self.get_conn()
                .instances()
                .delete(project=self.gcp_project, zone=self.gcp_zone, instance=name)
                .execute()
            )
            if wait:
                self.wait_for_operation(operation["name"])
            else:
                return operation["name"]
        except HttpError as e:
            if e.resp.status == 404:
                return None
            else:
                raise

    def delete_instances(self, timeout_in_minutes=60 * 12):
        instances = self.list_instances()

        instances = [
            x
            for x in instances
            if x.get("labels", {}).get("airflow", "") == "true"
            and x.get("labels", {}).get("env", "") == ENV_SHORT_NAME
            and x["status"] == "RUNNING"
        ]

        for instance in instances:
            creation = dateutil.parser.parse(instance["creationTimestamp"])
            now = datetime.datetime.now(pytz.utc)
            instance_life_minutes = (now - creation) / datetime.timedelta(minutes=1)
            if instance_life_minutes > timeout_in_minutes:
                self.__delete_instance(instance["name"])

    def wait_for_operation(self, operation):
        self.log.info(f"Waiting for operation {operation} to finish ...")
        retry = False
        while True:
            try:
                result = (
                    self.get_conn()
                    .zoneOperations()
                    .get(
                        project=self.gcp_project,
                        zone=self.gcp_zone,
                        operation=operation,
                    )
                    .execute()
                )
                if result["status"] == "DONE":
                    if "error" in result:
                        raise Exception(result["error"])
                    return result

                time.sleep(60)
            except Exception as e:
                if not retry:
                    self.log.info("Got except... sleep...")
                    time.sleep(60 * 5)
                    retry = True
                else:
                    raise e
