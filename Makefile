.PHONY: install-dev lint test

install-dev:
	python -m pip install -r requirements-dev.txt

lint:
	ruff check .

test:
	pytest
