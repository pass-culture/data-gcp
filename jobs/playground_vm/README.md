```rm ~/.ssh/known_hosts```

cd ~/data-gcp
```
export PATH=/opt/conda/bin:/opt/conda/condabin:+$PATH
export ENV_SHORT_NAME=...
export GCP_PROJECT_ID=passculture-data-...
conda init zsh && source ~/.zshrc && conda activate data-gcp
```
cd jobs/playground-vm
pip install -r jobs/playground_vm/requirements.txt

ipython kernel install --user --name=data-gcp   

nvidia-smi