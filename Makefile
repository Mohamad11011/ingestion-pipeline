.PHONY: install up down logs test scrape

PYTHON ?= python
export PYTHONPATH := src

install:
	$(PYTHON) -m pip install -e ".[dev]"

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
	cd scrapy_project && $(PYTHON) -m scrapy crawl workplace
