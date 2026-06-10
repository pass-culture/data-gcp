# Airflow Orchestration

Repository for orchestrating workflows using Airflow with Kubernetes deployment.

---

## 📁 Project Structure

* **`airflow/`** – Contains Airflow configuration files.
* **`dags/`** – DAGs deployed to a GCS bucket, accessible by the Airflow instance. (See [DAG Directory Structure](#dags-directory-structure))
* **`k8s-airflow/`** – Kubernetes configs to launch Airflow. (See [Related Notion Doc](https://www.notion.so/passcultureapp/AIRFLOW-Kubernetes-1a4ad4e0ff988184b503ec43c9dd2691))
* **`plugins/`** – Custom Airflow plugins (e.g. dbt documentation).
* **`tests/`** – Unit and integration tests.

---

## 📂 DAGs Directory Structure

Inside the `dags/` directory:

* **`jobs/`** – DAG definition files.
* **`dependencies/`** – DAG dependency modules.
* **`common/`** – Shared components like hooks, macros, operators.
* **`data_gcp_dbt/`** – Files related to dbt integration with GCP.

---

## 🚀 DAG Deployment & Execution (Kubernetes)

### 🔄 Automatic Deployment

When merging to `master` or `production`, DAGs are automatically deployed to Airflow on k8s cluster using GitHub Actions.
See [CD Documentation](../README.md#cd) for more.

Deployment process:

* Updates modified files in the bucket `airflow-data-bucket-{ENV}`.
* Confirms that Airflow successfully loads the DAGs.

### 🛠️ Trigger DAGs Manually

To manually access or trigger DAGs, use the following Airflow instances:

* **EHP**: [https://airflow-{env}.data.ehp.passculture.team](https://airflow-{env}.data.ehp.passculture.team)
* **Production**: [https://airflow.data.passculture.team](https://airflow.data.passculture.team)

### 🧪 Testing a DAG from your local branch

You can deploy a personal test copy of any DAG to a live Airflow environment without affecting the production version. The test copy gets a unique `dag_id`, has `schedule_interval=None`, and is automatically purged after 14 days.

```bash
cd orchestration/

# Create test copy and push to dev (default)
make test-dag DAG=my_dag.py

# Push to stg or prod
make test-dag DAG=my_dag.py ENV=stg
```

See [`scripts/HOW-TO-TEST-DAGS-FROM-LOCAL-MACHINE.md`](scripts/HOW-TO-TEST-DAGS-FROM-LOCAL-MACHINE.md) for the full workflow.

---

## 🧪 Local Airflow Setup (with Docker)

To run Airflow locally for development:

---

### 🔐 Prerequisites: GCP Auth & Environment Variables

1. Retrieve the GCP certificate of your machine and place it in the Airflow configuration directory:

   * Locate the **bundled/combined** certificate file for your machine.
     If unsure, check this [Notion page](https://www.notion.so/passcultureapp/Proxyfication-des-outils-du-pass-d1f0da09eafb4158904e9197bbe7c1d4?pvs=4#10cad4e0ff98805ba61efcea26075d65).
     * If you don't have the certificate, ask a team member from the DE or DS team.
   * Place it in:

     ```bash
     /airflow/etc/nscacert_combined.pem
     ```

2. **Environment file setup**:

   * Copy `.env.template` to `orchestration/.env`
   * Update variable values accordingly.

3. **Kubernetes - running containers on cluster from local airflow instance**:
  * make sure to have kubectl, kubens installed
  * authenticate using your admin account
  * connect to the cluster: `gcloud container clusters get-credentials data-gke-cluster-ehp --dns-endpoint --region europe-west1 --project passculture-data-ehp`
  * pro tip: k9s is very helpful to see what is happening in the cluster

#### 🛠️ Environment Variable Tips

* `_AIRFLOW_WWW_USER_USERNAME` / `_AIRFLOW_WWW_USER_PASSWORD`: set arbitrarily
* `AIRFLOW__CORE__FERNET_KEY` / `AIRFLOW__WEBSERVER__SECRET_KEY`: arbitrary strings
* `NETWORK_MODE`:

  * `"proxy"` if using a proxy
  * `"default"` otherwise
* For environment:

  * **dev**:

    ```env
    ENV_SHORT_NAME=dev
    GOOGLE_CLOUD_PROJECT=passculture-data-ehp
    ```

  * **stg**:

    ```env
    ENV_SHORT_NAME=stg
    GOOGLE_CLOUD_PROJECT=passculture-data-ehp
    ```

---

## 🏗️ Build & Run

### 🔧 Build the Docker image

⚠️ This will ask you to authenticate with GCP with your gadmin account.

```sh
make build
```

### ▶️ Start Airflow

⚠️ This will ask you to authenticate with GCP with your gadmin account.

```sh
make start
```

Then access the Airflow UI at:

```sh
http://localhost:8080
```

---

## 🛑 Stop Services

```sh
make stop
```

---

## 🔁 Update Environment Variables

After modifying `orchestration/.env`, rebuild with:

```sh
make build_with_cache
```

---

## 🧹 Troubleshooting

### View logs from containers

```sh
make show_airflow_logs
```

---

## 🐳 Dockerfile Tips

### Build with specific `NETWORK_MODE`

```sh
docker build -t <docker-image-name> <dockerfile-path> --build-arg NETWORK_MODE=<proxy|default>
```

### Build a specific target (multi-stage)

```sh
docker build --no-cache -f Dockerfile --target <target-name> .
```
