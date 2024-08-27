## Delete all compiled Python files
.PHONY: clean
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete

## Create a virtual environment named 'env' and activate it
.PHONY: venv
venv:			## Create a virtual environment
	@echo "Creating virtualenv ..."
	@rm -rf .venv
	@python3 -m venv .venv
	@./.venv/bin/pip install -U pip
	@echo
	@echo "Run 'source .venv/bin/activate' to enable the environment"

## Activate the virtual environment
.PHONY: activate
activate:		## Activate the virtual environment
	@echo "Activating virtualenv ..."
	. .venv/bin/activate

## Remove the virtual environment
.PHONY: remove_environment
remove_environment:
	rm -rf .venv
	@echo "Virtual environment '.venv' removed"

## Install requirements on current environment
.PHONY: prepare_environment
prepare_environment:
	pip install -r requirements.txt

## Run linter
.PHONY: lint
lint:
	@echo "Running linter ..."
	@flake8 . --ignore=E402,E129,W504

## Install pre-commit
.PHONY: install_pre_commit
install_pre_commit:
	pre-commit install
	pre-commit autoupdate

# Setup everything
.PHONY: setup
setup: venv activate prepare_environment install_pre_commit

.PHONY: help
help:
	@echo "$$(tput bold)Available rules:$$(tput sgr0)"
	@echo
	@sed -n -e "/^## / { \
		h; \
		s/.*//; \
		:doc" \
		-e "H; \
		n; \
		s/^## //; \
		t doc" \
		-e "s/:.*//; \
		G; \
		s/\\n## /---/; \
		s/\\n/ /g; \
		p; \
	}" ${MAKEFILE_LIST} \
	| LC_ALL='C' sort --ignore-case \
	| awk -F '---' \
		-v ncol=$$(tput cols) \
		-v indent=19 \
		-v col_on="$$(tput setaf 6)" \
		-v col_off="$$(tput sgr0)" \
	'{ \
		printf "%s%*s%s ", col_on, -indent, $$1, col_off; \
		n = split($$2, words, " "); \
		line_length = ncol - indent; \
		for (i = 1; i <= n; i++) { \
			line_length -= length(words[i]) + 1; \
			if (line_length <= 0) { \
				line_length = ncol - indent - length(words[i]) - 1; \
				printf "\n%*s ", -indent, " "; \
			} \
			printf "%s ", words[i]; \
		} \
		printf "\n"; \
	}' \
	| more $(shell test $(shell uname) = Darwin && echo '--no-init --raw-control-chars')
