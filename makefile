FILES := $(shell git diff --name-only --diff-filter=AM $$(git merge-base origin/main HEAD) -- \*.py)


.PHONY: test
test:
	pytest

.PHONY: lint
lint:
	flake8 --filename ./$(FILES) --max-complexity=10 --ignore=E501,W503

.PHONY: format
format:
ifneq ("$(FILES)"," ")
	black $(FILES)
	isort $(FILES)
endif

.PHONY: tidy
tidy: format lint

