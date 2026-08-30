# Decision Ledger developer Makefile (GNU make).
#
# Tested on macOS/Linux; on Windows use WSL or run the underlying commands
# directly (see the README "Development" section). Targets mirror the CI gates
# the repo enforces: black, isort, flake8, mypy --strict, pytest.

PYTHON ?= python
PIP    ?= $(PYTHON) -m pip

.PHONY: all install test cov format check-format lint typecheck check sdist clean

all: check

## Editable install with runtime + dev/test extras.
install:
	$(PIP) install -e ".[dev]"

## Run the full test suite (micro-benchmarks excluded from default runs).
test:
	$(PYTHON) -m pytest -k "not benchmark"

## Run the suite under branch coverage and report.
cov:
	$(PYTHON) -m coverage run --branch -m pytest -k "not benchmark"
	$(PYTHON) -m coverage report -m

## Auto-format with black then isort.
format:
	$(PYTHON) -m black src tests
	$(PYTHON) -m isort src tests

## Verify formatting without modifying files.
check-format:
	$(PYTHON) -m black --check src tests
	$(PYTHON) -m isort --check --diff src tests

## Lint (flake8, configured in pyproject.toml).
lint:
	$(PYTHON) -m flake8 src tests

## Type-check the package and examples with mypy --strict.
typecheck:
	$(PYTHON) -m mypy src/decision_ledger src/examples

## Full pre-release gate: format check + lint + typecheck + tests.
check: check-format lint typecheck test

## Build sdist + wheel into dist/ (requires the `build` package).
sdist:
	$(PYTHON) -m build

## Remove build artifacts and tool caches.
clean:
	rm -rf build dist *.egg-info .coverage htmlcov .pytest_cache .mypy_cache .benchmarks