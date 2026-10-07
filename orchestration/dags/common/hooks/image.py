from dataclasses import dataclass

from jinja2 import DictLoader, Environment

TEMPLATES = {
    # --- Reusable Sub-Templates ---
    "preamble": """
#!/bin/bash
set -euo pipefail

echo 'CC=gcc' | sudo tee -a /etc/environment
""",
    "setup_airflow_user": """
# Ensure airflow user exists, create home dir, and set permissions
if ! id -u airflow >/dev/null 2>&1; then
  sudo useradd -m -s /bin/bash airflow
fi

sudo mkdir -p /home/airflow
sudo chown -R airflow:airflow /home/airflow
sudo usermod -aG docker airflow 2>/dev/null || true
""",
    "install_docker": """
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
""",
    # --- Monitoring Blocks ---
    "ops_agent_basic": """
curl -sSO https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
sudo bash add-google-cloud-ops-agent-repo.sh --also-install
""",
    "ops_agent_dcgm": """
{% include 'ops_agent_basic' %}

# Non-blocking DCGM setup (`set +e` prevents monitoring failures from breaking VM boot).
# DLVM base image lacks `datacenter-gpu-manager` by default, so we attach NVIDIA's CUDA repo.
# Debug via `journalctl -u google-startup-scripts` or `systemctl status nvidia-dcgm`
set +e
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
    # --- Main Entrypoints ---
    "cpu_startup": """
{% include 'preamble' %}
{% include 'install_docker' %}
{% include 'setup_airflow_user' %}

{% if enable_monitoring %}
{% include 'ops_agent_basic' %}
{% endif %}
""",
    "gpu_startup": """
{% include 'preamble' %}

# Required for Triton's JIT compilation of custom CUDA kernels
sudo apt-get update -qq
sudo apt-get install -y -qq build-essential python3.10-dev

{% include 'setup_airflow_user' %}

{% if enable_monitoring %}
{% include 'ops_agent_dcgm' %}
{% endif %}
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
