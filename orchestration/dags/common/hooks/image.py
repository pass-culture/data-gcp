from dataclasses import dataclass


@dataclass
class CPUImage:
    source_image: str = (
        "projects/deeplearning-platform-release/global/images/tf-latest-cpu-v20230615"
    )
    startup_script: str = None
    startup_script_wait_time: int = 30


@dataclass
class TFGPUImage:
    source_image: str = (
        "projects/deeplearning-platform-release/global/images/tf-ent-latest-gpu-v20230925-debian-11-py310"
    )
    startup_script: str = """
        #!/bin/bash
        sudo /opt/deeplearning/install-driver.sh
    """
    startup_script_wait_time: int = 180


@dataclass
class TorchGPUImage:
    source_image: str = (
        "projects/deeplearning-platform-release/global/images/pytorch-latest-gpu-v20230925-debian-11-py310"
    )
    startup_script: str = """
        #!/bin/bash
        sudo /opt/deeplearning/install-driver.sh
    """
    startup_script_wait_time: int = 180


MACHINE_TYPE = {
    "cpu": CPUImage,
    "gpu": TFGPUImage,
    "gpu-tensorflow": TFGPUImage,
    "gpu-torch": TorchGPUImage,
}
