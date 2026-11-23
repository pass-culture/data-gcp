from dataclasses import dataclass
from pathlib import Path

from jinja2 import Environment, FileSystemLoader

STARTUP_SCRIPTS_DIR = Path(__file__).parent / "startup_scripts"

jinja_env = Environment(loader=FileSystemLoader(STARTUP_SCRIPTS_DIR))


def _render_script(template_name: str, **kwargs) -> str:
    """Helper to render Jinja templates into string defaults at module load time."""
    return jinja_env.get_template(template_name).render(**kwargs)


@dataclass
class CPUImage:
    source_image: str = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts"
    startup_script_wait_time: int = 90
    enable_monitoring: bool = True
    startup_script: str = _render_script("cpu_startup.sh.j2", enable_monitoring=True)


@dataclass
class TFGPUImage:
    source_image: str = "projects/deeplearning-platform-release/global/images/family/common-cu129-ubuntu-2204-nvidia-580"
    startup_script_wait_time: int = 240
    enable_monitoring: bool = True
    dcgm_receiver_version: int = 2
    startup_script: str = _render_script(
        "gpu_startup.sh.j2", enable_monitoring=True, dcgm_receiver_version=2
    )


MACHINE_TYPE = {
    "cpu": CPUImage,
    "gpu": TFGPUImage,
}
