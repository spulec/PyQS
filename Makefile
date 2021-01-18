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
	AWS_DEFAULT_REGION='us-east-1' nosetests --with-coverage --cover-package=$(PACKAGE) \
		--cover-branches --cover-erase --verbosity=2 && pycodestyle; \

test: prepare
	@make run_test

clean:
	@echo "Removing garbage..."
	@find . -name '*.pyc' -delete
	@find . -name '*.so' -delete
	@find . -name __pycache__ -delete
	@rm -rf .coverage *.egg-info *.log build dist MANIFEST yc

publish: clean
	rm -rf dist
	python -m pep517.build --source --binary .
	twine upload dist/*
