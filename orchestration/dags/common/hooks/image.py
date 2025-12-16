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
    # Updated 2025-04: TF 2.17 + CUDA 12.3 image from ml-images project (was tf-ent-2-14-cu118 DEPRECATED)
    source_image: str = "projects/ml-images/global/images/c1-deeplearning-tf-2-17-cu123-v20250205-debian-11-py310"
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
