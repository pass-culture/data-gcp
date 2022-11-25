install:
	pyenv install 3.7.7 -s
	pyenv local 3.7.7
	pip install --upgrade pip
	(pyenv virtualenv data-gcp || echo "pyenv-virtualenv data-gcp already exists") && pyenv local data-gcp && pip install --upgrade pip && pip install -r linter-requirements.txt
	cd recommendation/api && (pyenv virtualenv data-gcp-api || echo "pyenv-virtualenv data-gcp-api already exists") && pyenv local data-gcp-api && pip install --upgrade pip && pip install -r api-dev-requirements.txt
	cd algo_training && (pyenv virtualenv data-gcp-algo-training || echo "pyenv-virtualenv data-gcp-algo-training already exists") && pyenv local data-gcp-algo-training && pip install --upgrade pip && pip install -r requirements.txt
	cd diversification_kpi && (pyenv virtualenv data-gcp-diversification-kpi || echo "pyenv-virtualenv data-gcp-diversification-kpi already exists") && pyenv local data-gcp-diversification-kpi && pip install --upgrade pip && pip install -r requirements.txt