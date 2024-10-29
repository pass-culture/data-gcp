from dataclasses import dataclass


@dataclass
class CPUImage:
    source_image: str = "projects/deeplearning-platform-release/global/images/tf-ent-2-14-cpu-v20240922-py310"
    startup_script: str = None
    startup_script_wait_time: int = 30


@dataclass
class TFGPUImage:
    source_image: str = "projects/deeplearning-platform-release/global/images/tf-ent-2-14-cu118-v20240922-py310"
    startup_script: str = """
        #!/bin/bash
        sudo /opt/deeplearning/install-driver.sh
    """
    startup_script_wait_time: int = 180


MACHINE_TYPE = {
    "cpu": CPUImage,
    "gpu": TFGPUImage,
}
