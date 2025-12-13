.DEFAULT_GOAL := help
.PHONY: help sync test lint format fmt-stm fmt-stm-check type-check docs build clean vscode-install

# every .stm machine in the repo (excludes the virtualenv)
STM_FILES := $(shell find . -path ./.venv -prune -o -name '*.stm' -print)

# the VSCode extension lives in its own (bun-managed) toolchain
VSCODE_SRC := editor/vscode
VSCODE_EXT_DIR ?= $(HOME)/.vscode/extensions

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

sync: ## Install/refresh the virtualenv from pyproject + lock
	uv sync

test: ## Run the test suite
	uv run pytest

lint: ## Check style and imports without modifying files
	uv run ruff check .
	uv run ruff format --check .
	$(MAKE) fmt-stm-check

format: ## Auto-format code and fix imports
	uv run ruff format .
	uv run ruff check --fix .

fmt-stm: ## Canonically format every .stm DSL file in place
	uv run harel-fmt $(STM_FILES)

fmt-stm-check: ## Check that every .stm file is canonically formatted (no writes)
	uv run harel-fmt --check $(STM_FILES)

type-check: ## Run static type checking
	uv run mypy

docs: ## Build the HTML documentation (Sphinx + MyST)
	uv run sphinx-build -b html docs docs/_build/html

build: ## Build the sdist and wheel
	uv build

vscode-install: ## Install the VSCode extension unpacked into ~/.vscode/extensions (needs bun; no npm/vsce)
	cd $(VSCODE_SRC) && bun install
	@ver=$$(cd $(VSCODE_SRC) && bun -e 'process.stdout.write(require("./package.json").version)'); \
	dest="$(VSCODE_EXT_DIR)/harel-$$ver"; \
	echo "installing extension to $$dest"; \
	rm -rf "$$dest"; mkdir -p "$$dest"; \
	cp -R $(VSCODE_SRC)/package.json $(VSCODE_SRC)/language-configuration.json \
	      $(VSCODE_SRC)/README.md $(VSCODE_SRC)/bun.lock \
	      $(VSCODE_SRC)/src $(VSCODE_SRC)/syntaxes $(VSCODE_SRC)/media \
	      $(VSCODE_SRC)/node_modules "$$dest/"; \
	echo "done — reload the VS Code window (Developer: Reload Window) and open a .stm"

clean: ## Remove caches and build artifacts
	rm -rf dist build *.egg-info src/*.egg-info .ruff_cache
	find . -path ./.venv -prune -o -type d -name __pycache__ -exec rm -rf {} +
	find . -path ./.venv -prune -o -type d -name .pytest_cache -exec rm -rf {} +
