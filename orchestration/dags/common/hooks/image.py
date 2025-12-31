from dataclasses import dataclass


@dataclass
class CPUImage:
    # Updated 2025-04: M129 image from ml-images project (was M125 tf-ent-2-14 DEPRECATED)
    source_image: str = "projects/ml-images/global/images/c0-deeplearning-common-cpu-v20250325-debian-11-py310"
    startup_script: str = """
        #!/bin/bash
        echo 'CC=gcc' | sudo tee -a /etc/environment
        curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
        sudo bash add-google-cloud-ops-agent-repo.sh --also-install
        """
    startup_script_wait_time: int = 30


@dataclass
class TFGPUImage:
    source_image: str = "projects/deeplearning-platform-release/global/images/tf-ent-2-14-cu118-v20240922-py310"
    startup_script: str = """
        #!/bin/bash
        echo 'CC=gcc' | sudo tee -a /etc/environment
        sudo /opt/deeplearning/install-driver.sh
        curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
        sudo bash add-google-cloud-ops-agent-repo.sh --also-install
    """
    startup_script_wait_time: int = 180


MACHINE_TYPE = {
    "cpu": CPUImage,
    "gpu": TFGPUImage,
}
