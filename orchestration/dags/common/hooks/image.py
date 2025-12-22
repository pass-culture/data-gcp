from dataclasses import dataclass


@dataclass
class CPUImage:
    source_image: str = "projects/deeplearning-platform-release/global/images/tf-ent-2-14-cpu-v20240922-py310"
    startup_script: str = """
        #!/bin/bash
        echo 'CC=gcc' | sudo tee -a /etc/environment
        curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
        sudo bash add-google-cloud-ops-agent-repo.sh --also-install
        conda config --set auto_activate_base false
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
