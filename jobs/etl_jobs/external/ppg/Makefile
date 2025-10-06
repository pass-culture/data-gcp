PYTHON := python3.10
VENV := .venv

ENV_VARS := \
	GCP_PROJECT="passculture-data-prod" \
	ENV_SHORT_NAME="prod"

.PHONY: help venv install clean

help:
	@echo "venv     - Create venv with env backup/restore"
	@echo "install  - Create venv and install dependencies"
	@echo "clean    - Remove venv"

venv:
	uv venv --python=$(PYTHON)
	@echo "" >> $(VENV)/bin/activate
	@for var in $(ENV_VARS); do \
		name=$${var%%=*}; \
		value=$${var#*=}; \
		echo "_OLD_$${name}=\"\$$$${name}\"" >> $(VENV)/bin/activate; \
		echo "export $${name}=$${value}" >> $(VENV)/bin/activate; \
	done
	@echo "" >> $(VENV)/bin/activate
	@echo "_orig_deactivate=\"\$$(declare -f deactivate)\"" >> $(VENV)/bin/activate
	@echo "" >> $(VENV)/bin/activate
	@echo "deactivate() {" >> $(VENV)/bin/activate
	@for var in $(ENV_VARS); do \
		name=$${var%%=*}; \
		echo "  [ -n \"\$$_OLD_$${name}\" ] && export $${name}=\"\$$_OLD_$${name}\" || unset $${name}" >> $(VENV)/bin/activate; \
	done
	@for var in $(ENV_VARS); do \
		name=$${var%%=*}; \
		echo "  unset _OLD_$${name}" >> $(VENV)/bin/activate; \
	done
	@echo "  unset -f deactivate" >> $(VENV)/bin/activate
	@echo "  eval \"\$$_orig_deactivate\"" >> $(VENV)/bin/activate
	@echo "  deactivate" >> $(VENV)/bin/activate
	@echo "  unset _orig_deactivate" >> $(VENV)/bin/activate
	@echo "}" >> $(VENV)/bin/activate

install: venv
	uv pip install -r requirements.txt --python $(VENV)/bin/python
	@echo "✓ Activate with: source $(VENV)/bin/activate"

clean:
	rm -rf $(VENV)
