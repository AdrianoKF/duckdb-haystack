.PHONY: test lint format typecheck cov clean

## Run tests
test:
	uv run pytest tests

## Run linters (ruff check + format check)
lint:
	uv run ruff format --diff .
	uv run ruff check .

## Format code
format:
	uv run ruff format .
	uv run ruff check --fix .

## Run type checker
typecheck:
	uv run basedpyright src/haystack_integrations

## Run tests with coverage
cov:
	uv run coverage run -m pytest tests
	-uv run coverage combine
	uv run coverage report

## Clean build artifacts
clean:
	rm -rf dist/ build/ *.egg-info .coverage .coverage.* htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type d -name .pytest_cache -exec rm -rf {} + 2>/dev/null || true

## Run all checks (lint + typecheck + test)
all: lint typecheck test
