.PHONY: install install-orchestration up down logs test scrape transform dagster

PYTHON ?= python
START_DATE ?= 2024-01-01
END_DATE ?= 2024-02-01
DAGSTER_HOME ?= $(CURDIR)/.dagster
export PYTHONPATH := src
export DAGSTER_HOME

install:
	$(PYTHON) -m pip install -e ".[dev]"

install-orchestration:
	$(PYTHON) -m pip install -e ".[dev,orchestration]"

up:
	docker compose --env-file .env.example up -d
	docker compose --env-file .env.example ps

down:
	docker compose --env-file .env.example down

logs:
	docker compose --env-file .env.example logs -f --tail=80

test:
	$(PYTHON) -m pytest

scrape:
	cd scrapy_project && $(PYTHON) -m scrapy crawl workplace -a start_date=$(START_DATE) -a end_date=$(END_DATE)

transform:
	$(PYTHON) -m transformation.transformer --start-date $(START_DATE) --end-date $(END_DATE)

dagster:
	$(PYTHON) -c "from pathlib import Path; Path(r'''$(DAGSTER_HOME)''').mkdir(parents=True, exist_ok=True)"
	$(PYTHON) -m dagster dev -f orchestration/dagster/definitions.py
