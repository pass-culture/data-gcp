# ────────────────────────────────────────────────────────
# Configuration
# ────────────────────────────────────────────────────────

# Detect user’s login shell for interactive alias loading
USER_SHELL   := $(shell echo $$SHELL)

# Python version
PYTHON_VERSION := python3.11

# Virtualenv and env file
VENV_DIR     := .dbt_clickhouse_env
ENV_FILE     := .env.local

# Path to the venv’s activate script
ACTIVATE     := $(VENV_DIR)/bin/activate

# Where we’ll store the port-forward PID
PID_FILE     := .clickhouse_portforward.pid


.PHONY: init patch_activate setup clickhouse_connect clickhouse_disconnect
# ────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────

# Append env-sourcing snippet
patch_activate:
	@echo "🔧 Ensuring $(ACTIVATE) auto-loads $(ENV_FILE)…"
	@if ! grep -q "# >>> load project $(ENV_FILE)" $(ACTIVATE); then \
	  echo '' >> $(ACTIVATE); \
	  echo '# >>> load project $(ENV_FILE) >>>' >> $(ACTIVATE); \
	  echo 'if [ -f "$$VIRTUAL_ENV/../$(ENV_FILE)" ]; then' >> $(ACTIVATE); \
	  echo '  set -o allexport' >> $(ACTIVATE); \
	  echo '  . "$$VIRTUAL_ENV/../$(ENV_FILE)"' >> $(ACTIVATE); \
	  echo '  set +o allexport' >> $(ACTIVATE); \
	  echo 'fi' >> $(ACTIVATE); \
	  echo '# <<< end project $(ENV_FILE) <<<' >> $(ACTIVATE); \
	  echo "✔ Patched $(ACTIVATE)"; \
	else \
	  echo "✔ $(ACTIVATE) already patched"; \
	fi
# ────────────────────────────────────────────────────────
# Setup
# ────────────────────────────────────────────────────────
init:
	@echo "🛠  Bootstrapping .env and virtualenv"
	@cp -n .env.template $(ENV_FILE) || true
	@echo "➤ Make sure to fill in the .env.local file using values from gcp secret manager"

setup:
	@echo "➤ Creating venv at $(VENV_DIR)"
	@uv venv $(VENV_DIR) --python=$(PYTHON_VERSION)
	@echo "🛠  Bootstrapping .env.local env variables into virtualenv"
	make patch_activate
	@echo "➤ Installing dependencies inside venv"
	@$(USER_SHELL) -ic '\
		source "$(ACTIVATE)" && \
		uv pip install -r requirements.txt \
	'

# ────────────────────────────────────────────────────────
# ClickHouse Tunnel
# ────────────────────────────────────────────────────────

connect_clickhouse:
	@echo "🔗 Setting up proxy & context…"
	@$(USER_SHELL) -ic "ctxdataehp"
	@kubens clickhouse-dev
	@echo "🔌 Port-forwarding svc/clickhouse-svc → localhost:8123"
	@kubectl port-forward svc/clickhouse-svc 8123:8123 \
		> /dev/null 2>&1 & \
		echo $$! > $(PID_FILE)
	@echo "✔ Tunnel PID: $$(cat $(PID_FILE))"
	@echo "➜ Remember to activate your venv: source $(ACTIVATE)"

disconnect_clickhouse:
	@echo "🛑 Tearing down ClickHouse tunnel…"
	@if [ -f $(PID_FILE) ]; then \
		kill $$(cat $(PID_FILE)) && rm -f $(PID_FILE); \
		echo "✔ Disconnected"; \
	else \
		echo "⚠ No PID file – nothing to kill"; \
	fi
