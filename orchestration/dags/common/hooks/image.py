from dataclasses import dataclass

from jinja2 import DictLoader, Environment

TEMPLATES = {
    "cpu_startup": """
#!/bin/bash
set -euo pipefail

echo 'CC=gcc' | sudo tee -a /etc/environment
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential git curl

{% if enable_monitoring %}
{% include 'ops_agent_basic' %}
{% endif %}
""",
    "gpu_startup": """
#!/bin/bash
set -euo pipefail

echo 'CC=gcc' | sudo tee -a /etc/environment

# Required for Triton's JIT compilation of custom CUDA kernels (e.g. the
# torch._native bmm_outer_product op used by Gemma3 RoPE): triton shells out
# to gcc with -I<python include dir>, which fails with "Python.h: No such
# file or directory" if the -dev headers aren't installed. The DLVM base
# image doesn't ship them by default.
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential python3.10-dev

{% if enable_monitoring %}
{% include 'ops_agent_dcgm' %}
{% endif %}
""",
    "ops_agent_basic": """
curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
sudo bash add-google-cloud-ops-agent-repo.sh --also-install
""",
    "ops_agent_dcgm": """
{% include 'ops_agent_basic' %}

# Best-effort: a failure setting up DCGM/GPU monitoring shouldn't abort the
# whole startup script (the job itself doesn't depend on it). Everything below
# runs with `set +e` for that reason; check `journalctl -u google-startup-scripts`
# / `systemctl status nvidia-dcgm google-cloud-ops-agent` on the VM to debug.
set +e

# datacenter-gpu-manager isn't in any repo this image ships by default
# (confirmed via a live GCE spike: apt-cache madison returned nothing,
# which — combined with the old unconditional `set -e` — silently aborted
# the entire startup script before the DCGM install or the ops-agent config
# below ever ran). NVIDIA's CUDA network repo is what actually hosts it.
if ! apt-cache madison datacenter-gpu-manager | grep -q '3.3.'; then
    wget -q https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2204/x86_64/cuda-keyring_1.1-1_all.deb -O /tmp/cuda-keyring.deb
    sudo dpkg -i /tmp/cuda-keyring.deb
    sudo apt-get update -qq
fi
DCGM_VERSION=$(apt-cache madison datacenter-gpu-manager | grep '3.3.' | head -1 | awk '{print $3}')
sudo apt-get install -y datacenter-gpu-manager=${DCGM_VERSION}
sudo systemctl enable --now nvidia-dcgm

sudo tee /etc/google-cloud-ops-agent/config.yaml > /dev/null << 'EOF'
metrics:
  receivers:
    dcgm:
      type: dcgm
      receiver_version: {{ dcgm_receiver_version }}
  service:
    pipelines:
      dcgm:
        receivers:
          - dcgm
EOF

sudo systemctl restart google-cloud-ops-agent
set -e
""",
}

jinja_env = Environment(loader=DictLoader(TEMPLATES))


def _render_script(template_name: str, **kwargs) -> str:
    """Helper to render Jinja templates into string defaults at module load time."""
    return jinja_env.get_template(template_name).render(**kwargs)


@dataclass
class CPUImage:
    # tf-ent-2-14-cpu was deprecated by Google, and no non-deprecated CPU-only
    # DLVM family exists anymore in this image generation. Jobs here only ever
    # relied on `uv`-managed venvs (no conda/preinstalled framework
    # dependency), so moving to a plain, continuously-maintained OS image
    # removes the DLVM deprecation-cycle risk entirely. Confirmed via a live
    # GCE spike: curl/git present by default, gcc needs an explicit install,
    # and `uv sync` (including a from-source pandas build on 3.13) succeeds.
    source_image: str = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts"
    startup_script_wait_time: int = 90
    enable_monitoring: bool = True
    startup_script: str = _render_script("cpu_startup", enable_monitoring=True)


@dataclass
class TFGPUImage:
    # tf-ent-2-14-cu118 was deprecated by Google and pinned torch to CUDA-13
    # wheels its driver couldn't run, silently falling back to CPU (~15x
    # slower). This family/nvidia-580 image ships a matching driver already
    # baked in (confirmed via a live GCE spike: nvidia-smi works cold, no
    # install-driver.sh present), and is a rolling `family/` reference so it
    # won't silently freeze in time again.
    source_image: str = "projects/deeplearning-platform-release/global/images/family/common-cu129-ubuntu-2204-nvidia-580"
    startup_script_wait_time: int = 240
    enable_monitoring: bool = True
    dcgm_receiver_version: int = 2
    startup_script: str = _render_script(
        "gpu_startup", enable_monitoring=True, dcgm_receiver_version=2
    )


MACHINE_TYPE = {
    "cpu": CPUImage,
    "gpu": TFGPUImage,
}
