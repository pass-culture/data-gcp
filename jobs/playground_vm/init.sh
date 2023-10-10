#! /bin/sh
export PATH=/opt/conda/bin:/opt/conda/condabin:+$PATH
export ENV_SHORT_NAME=dev
export GCP_PROJECT_ID=passculture-data-ehp
conda init zsh && source ~/.zshrc && conda activate data-gcp
pip install -r requirements.txt