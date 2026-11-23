# GCE startup scripts

Jinja templates rendered by `../image.py` into the `startup-script` metadata
for `CPUImage`/`TFGPUImage` VMs (see `orchestration/dags/common/hooks/gce.py`).
These are shared infrastructure: every GCE-based Airflow job that uses
`CPUImage`/`TFGPUImage` boots from the same templates, not a particular DAG.
`cpu_startup.sh.j2` and `gpu_startup.sh.j2` are the two entrypoints; everything
else is a snippet they `{% include %}`.

## Files

| file | included by | purpose |
|---|---|---|
| `cpu_startup.sh.j2` | `CPUImage` | entrypoint: preamble, airflow user, Docker, ops agent |
| `gpu_startup.sh.j2` | `TFGPUImage` | entrypoint: preamble, airflow user, Triton deps, DCGM |
| `preamble.sh.j2` | both entrypoints | shebang, `set -euo pipefail`, sets `CC=gcc` for native builds |
| `setup_airflow_user.sh.j2` | both entrypoints | creates `airflow` account, puts it in the `docker` group (see below) |
| `install_docker.sh.j2` | `cpu_startup.sh.j2` | installs Docker CE from Docker's own apt repo, not the distro's `docker.io` |
| `install_triton_deps.sh.j2` | `gpu_startup.sh.j2` | gcc + `python3.10-dev`, needed for Triton to JIT-compile custom CUDA kernels |
| `ops_agent_basic.sh.j2` | `cpu_startup.sh.j2`, `ops_agent_dcgm.sh.j2` | installs the Google Cloud Ops Agent (metrics/logging) |
| `ops_agent_dcgm.sh.j2` | `gpu_startup.sh.j2` | ops agent + best-effort NVIDIA DCGM GPU metrics (`set +e`: monitoring must not fail the boot) |

## `setup_airflow_user.sh.j2`

This file carries the two trickiest, most permission-related bits in the
whole directory - both were the direct cause of a production incident.

### Why only `docker`

The Google guest agent has its own stock default group list for SSH-key
-provisioned accounts - `adm,dip,docker,lxd,plugdev,video`, see the docs
below - and `instance_configs.cfg`'s `groups =` *replaces* that default
rather than appending to it, for *every* account the guest agent manages on
this VM, not just `airflow`.

Checked against everything that actually drives `airflow` over SSH here -
this directory, the shared job-management/polling code
(`../gce.py`'s `SSHGCEJobManager`/`DeferrableSSHGCEJobManager`,
`../../triggers/gce.py`'s `DeferrableSSHJobMonitorTrigger`, which
backgrounds a job the way `nohup` would and polls its status from files
under `~/airflow_jobs/<job_id>/`), and the DAGs inspected: only `docker` is
ever touched (`install_docker.sh.j2`, the `usermod` below). The rest of the
default - reading `/var/log`/`journalctl` (`adm`), dial-up (`dip`), the LXD
socket (`lxd`), USB (`plugdev`), `/dev/video*` (`video`) - is unused by any
of it (`mkdir`/`echo`/`cat`/`tail`/`kill -0`/`cp`/`rm -rf` on files `airflow`
already owns needs no supplementary group). Not a guarantee no DAG anywhere
could ever need one of these, only that nothing found so far does.

Also not neutral the other way: Google's own guest-agent security hardening
work calls out `adm`, `docker`, and `lxd` specifically as groups it should
*stop* auto-granting by default, since they're a privilege-escalation
surface for any account that gets this treatment.

One real trade-off: since this config is shared by every guest-agent
account on the VM, a human SSHing in to debug also loses passwordless `adm`
(reading logs without `sudo`) - `sudo` covers that instead.

- [Configure the guest agent - Google Cloud docs](https://docs.cloud.google.com/compute/docs/images/manage-guest-agent)
- [Remove OS Login users from admin groups - GoogleCloudPlatform/guest-oslogin#29](https://github.com/GoogleCloudPlatform/guest-oslogin/pull/29/files)

### Why `usermod` runs before the `chown`, and the `chown` is best-effort

These VMs are shared infrastructure: this startup script runs at boot, and
SSH becomes reachable before it finishes (installing Docker alone takes over
a minute) - so whatever DAG scheduled the VM can already be running its own
SSH-driven tasks **concurrently** with this script, writing into
`/home/airflow` (installing a venv, cloning a repo, a framework writing/
removing its own artifact temp files, etc).

`sudo chown -R airflow:airflow /home/airflow` walks that same tree. If it
happens to hit a file some concurrent task just deleted mid-walk, it exits
non-zero. `preamble.sh.j2` sets `set -euo pipefail`, so that failure used to
abort the **entire script on the spot** - including every line after it,
regardless of which job triggered the race.

This isn't hypothetical: happend the 2026-11-23,
when one job's own SSH task (installing its venv, logging an
mlflow artifact) happened to delete a temp file in `/home/airflow` at the
exact moment this `chown` walked past it. The script died there, so
`usermod -aG docker airflow` never ran, and that same job's own docker build
step ~10 minutes later failed with `permission denied ... docker.sock` -
with no code or image change anywhere, just bad luck in the race's timing.
Nothing about this is specific to that one job: any SSH-driven process
writing into `/home/airflow` while this script is still running is exposed
to the same race. Fix: `usermod` now runs *before* the `chown`, and the
`chown` itself is `|| true`, so a filesystem race there can never again take
the rest of the script down with it.

## `install_triton_deps.sh.j2`

GPU-only. Triton (used by `torch.compile` and similar) JIT-compiles some
custom CUDA kernels to native code the first time they run - e.g. Gemma3
RoPE's `bmm_outer_product` op. To do that it shells out to `gcc`
(`build-essential`) with `-I<python include dir>`, which needs `Python.h`
(`python3.10-dev`, matching Ubuntu 22.04's system Python). The DLVM base
image ships neither by default, so without this the first op that needs
JIT compilation crashes at runtime instead of failing cleanly at boot.

## `ops_agent_dcgm.sh.j2`

- `set +e` / `set -e` bracket the whole block on purpose: GPU metrics aren't
  on the critical path, so a failure here (e.g. an apt mirror hiccup) must
  not take the VM boot down with it, unlike the Docker install above.
- The DLVM base image doesn't ship `datacenter-gpu-manager`, so NVIDIA's own
  CUDA apt repo is added first to get it.
- `DCGM_VERSION` is resolved dynamically (`apt-cache madison ... | grep
  '3.3.'`) instead of pinning one build, so new 3.3.x patch releases get
  picked up automatically. To force a specific build, tighten that `grep`
  pattern (e.g. to `3.3.5`) rather than hardcoding the install line.
