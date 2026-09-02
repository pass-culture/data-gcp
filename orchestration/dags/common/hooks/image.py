from dataclasses import dataclass

from jinja2 import DictLoader, Environment

TEMPLATES = {
    "cpu_startup": """
#!/bin/bash
set -euo pipefail

echo 'CC=gcc' | sudo tee -a /etc/environment

sudo systemctl restart google-guest-agent || true

# Install Docker Engine from Docker's official apt repo (docker.io is unofficial).
# https://docs.docker.com/engine/install/ubuntu/
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential git ca-certificates curl gnupg

# Drop distro docker packages that would conflict with docker-ce (no-op if absent).
sudo apt-get remove -y -qq docker.io docker-doc docker-compose podman-docker containerd runc || true
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
sudo tee /etc/apt/sources.list.d/docker.sources > /dev/null <<EOF
Types: deb
URIs: https://download.docker.com/linux/ubuntu
Suites: $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}")
Components: stable
Architectures: $(dpkg --print-architecture)
Signed-By: /etc/apt/keyrings/docker.asc
EOF

sudo apt-get update -qq
sudo apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker.service containerd.service

# Add the SSH login user to the `docker` group so it can reach the docker socket
# without sudo. This must happen here, after the docker install
if id -u airflow >/dev/null 2>&1; then
  sudo usermod -aG docker airflow
fi

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
    source_image: str = "projects/ubuntu-os-cloud/global/images/family/ubuntu-2204-lts"
    startup_script_wait_time: int = 90
    enable_monitoring: bool = True
    startup_script: str = _render_script("cpu_startup", enable_monitoring=True)


@dataclass
class TFGPUImage:
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
