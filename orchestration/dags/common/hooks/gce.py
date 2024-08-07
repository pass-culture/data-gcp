from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
import googleapiclient.discovery
from googleapiclient.errors import HttpError
import pytz, json, os, time
import dateutil
import typing as t
import datetime
from common.hooks.image import CPUImage
from common.hooks.network import DefaultVPCNetwork
from common.config import (
    ENV_SHORT_NAME,
    GCP_REGION,
    GCE_ZONE,
    GCP_PROJECT_ID,
    GCE_SA,
)

DEFAULT_LABELS = {
    "env": ENV_SHORT_NAME,
    "terraform": "false",
    "airflow": "true",
    "keep_alive": "false",
    "job_type": "default",
}


class GCEHook(GoogleBaseHook):
    _conn = None

    def __init__(
        self,
        gcp_project: str = GCP_PROJECT_ID,
        gcp_zone: str = GCE_ZONE,
        gcp_region: str = GCP_REGION,
        gce_networks: t.List[DefaultVPCNetwork] = [DefaultVPCNetwork()],
        gce_sa: str = GCE_SA,
        source_image_type: CPUImage = CPUImage(),
        gcp_conn_id: str = "google_cloud_default",
        disk_size_gb: str = "100",
        delegate_to: str = None,
        impersonation_chain: str = None,
    ):
        self.gcp_project = gcp_project
        self.gcp_zone = gcp_zone
        self.gcp_region = gcp_region
        self.gce_networks = gce_networks
        self.gce_sa = gce_sa
        self.disk_size_gb = disk_size_gb
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
        labels={},
        gpu_count=0,
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
            labels=labels,
            wait=True,
            preemptible=preemptible,
            accelerator_types=accelerator_types,
            gpu_count=gpu_count,
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
        labels,
        gpu_count=0,
        metadata=None,
        wait=False,
        accelerator_types=[],
        preemptible=False,
    ):
        instance_type = "zones/%s/machineTypes/%s" % (self.gcp_zone, instance_type)
        gpu_counter = max([gpu_count] + [a_t["count"] for a_t in accelerator_types])
        accelerator_type = [
            {
                "acceleratorCount": [gpu_counter],
                "acceleratorType": "zones/%s/acceleratorTypes/%s"
                % (self.gcp_zone, a_t.get("name", "nvidia-tesla-t4")),
            }
            for a_t in accelerator_types
            if gpu_counter in [1, 2, 4]
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
                        "disk_size_gb": self.disk_size_gb,
                        "sourceImage": self.source_image_type.source_image,
                    },
                }
            ],
            # Specify VPC network interface
            "networkInterfaces": [
                {
                    "network": network.network_id,
                    "subnetwork": network.subnetwork_id,
                }
                for network in self.gce_networks
            ],
            "serviceAccounts": [
                {
                    "email": f"{self.gce_sa}@{self.gcp_project}.iam.gserviceaccount.com",
                    "scopes": ["https://www.googleapis.com/auth/cloud-platform"],
                }
            ],
            "metadata": {"items": metadata},
            "tags": {"items": ["training"]},
            "labels": dict({**DEFAULT_LABELS, **labels}),
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

    def delete_instances(self, job_type="default", timeout_in_minutes=60 * 12):
        instances = self.list_instances()

        instances = [
            x
            for x in instances
            if x.get("labels", {}).get("airflow", "") == "true"
            and x.get("labels", {}).get("env", "") == ENV_SHORT_NAME
            and not x.get("labels", {}).get("keep_alive", "false") == "true"
            and x.get("labels", {}).get("job_type", "default") == job_type
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
