# Airflow K8s Worker

This is a simple custom GCP Airflow Dockerfile development environment to match with our kubernetes deployment.

## Build
```bash
PROJECT_ID=passculture-data-{prod|ehp}
ENV_SHORT_NAME=prod|stg|dev

docker build -t gcr.io/${PROJECT_ID}/gke-data-airflow-${ENV_SHORT_NAME}:latest -f Dockerfile.k8s-worker .
```

## CI/CD

We use the `on_cd` workflow to build and push the image to GCR.
