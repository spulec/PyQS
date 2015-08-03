PACKAGE=pyqs
CUSTOM_PIP_INDEX=pypi

all: setup test

prepare: clean install_deps

setup: prepare

pre_commit: setup
	@pre-commit run --all-files

install_deps:
	@if [ -z $$VIRTUAL_ENV ]; then \
		echo "===================================================="; \
		echo "You're not running this from a virtualenv, wtf?"; \
		echo "ಠ_ಠ"; \
		echo "===================================================="; \
		exit 1; \
	fi

	@if [ -z $$SKIP_DEPS ]; then \
		echo "Installing missing dependencies..."; \
		[ -e development.txt  ] && pip install --quiet -r development.txt; \
	fi
	@pre-commit install
	@python setup.py develop &> .build.log

run_test:
	@echo "Running \033[0;32mtest suite\033[0m "; \
	nosetests --stop --with-coverage --cover-package=$(PACKAGE) \
		--cover-branches --cover-erase --verbosity=2; \

test: prepare
	@make run_test

clean:
	@echo "Removing garbage..."
	@find . -name '*.pyc' -delete
	@find . -name '*.so' -delete
	@find . -name __pycache__ -delete
	@rm -rf .coverage *.egg-info *.log build dist MANIFEST yc

publish: clean
	@if [ -e "$$HOME/.pypirc" ]; then \
		echo "Uploading to '$(CUSTOM_PIP_INDEX)'"; \
		python setup.py register -r "$(CUSTOM_PIP_INDEX)"; \
		python setup.py sdist upload -r "$(CUSTOM_PIP_INDEX)"; \
	else \
		echo "You should create a file called '.pypirc' under your home dir.\n"; \
		echo "That's the right place to configure 'pypi' repos.\n"; \
		exit 1; \
	fi
