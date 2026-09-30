The `gcp_launch_vm` Airflow DAG clones the repo and runs `uv sync` on the VM
for you. If you need to (re)install manually once connected:

```
export ENV_SHORT_NAME=...
export GCP_PROJECT_ID=passculture-data-...
cd data-gcp/jobs/playground_vm
uv sync
source .venv/bin/activate
```
