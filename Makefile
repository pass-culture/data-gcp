install:
	pyenv install 3.7.13 -s
	pyenv local 3.7.13
	pip install --upgrade pip
	MICROSERVICE_PATH=. VENV_NAME=data-gcp REQUIREMENTS_NAME=linter-requirements.txt make install_microservice
	MICROSERVICE_PATH=recommendation/api VENV_NAME=data-gcp-api REQUIREMENTS_NAME=api-dev-requirements.txt make install_microservice
	MICROSERVICE_PATH=algo_training VENV_NAME=data-gcp-algo-training REQUIREMENTS_NAME=requirements.txt make install_microservice
	MICROSERVICE_PATH=diversification_kpi VENV_NAME=data-gcp-diversification-kpi REQUIREMENTS_NAME=requirements.txt make install_microservice

install_microservice:
	cd $(MICROSERVICE_PATH) && (pyenv virtualenv $(VENV_NAME) || echo "pyenv-virtualenv $(VENV_NAME) already exists") && pyenv local $(VENV_NAME) && pip install --upgrade pip && pip install -r $(REQUIREMENTS_NAME)