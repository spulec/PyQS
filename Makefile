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

tag:
	@if [ $$(git rev-list $$(git describe --abbrev=0 --tags)..HEAD --count) -gt 0 ]; then \
		if [ $$(git log  -n 1 --oneline $$(git describe --abbrev=0 --tags)..HEAD CHANGELOG.md | wc -l) -gt 0 ]; then \
			git tag $$(python setup.py --version) && git push --tags || echo 'Version already released, update your version!'
		else \
			echo "CHANGELOG not updated since last release!"; \
			exit 1; \
		fi; \
	else \
		echo "No commits since last release!"; \
		exit 1;\
	fi

# latest_tag=$$(git describe --abbrev=0 --tags)
# number_of_commits_since_last_tag=$$(git rev-list $${latest_tag}..HEAD --count)
# number_of_changelog_commits_since_last_tag=$$(git log  -n 1 --oneline $$(git describe --abbrev=0 --tags)..HEAD CHANGELOG.md | wc -l)
# @if [ ${number_of_commits_since_last_tag} -gt 0 ]; then \
# 	if [ ${number_of_changelog_commits_since_last_tag} -gt 0 ]; then \
# 		git tag $$(python setup.py --version) && git push --tags || echo 'Version already released, update your setup.py version!' ;  \
# 	else \
# 		echo "CHANGELOG not updated since last release!"; \
# 		exit 1; \
# 	fi; \
# else \
# 	echo "No commits since last release!"; \
# 	exit 1;\
# fi
