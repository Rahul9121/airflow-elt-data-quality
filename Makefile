.PHONY: seed test lint report up down standalone pipeline-good pipeline-bad backfill ci-local

PYTHON ?= python
export AIRFLOW_HOME ?= $(CURDIR)/.airflow_home
export PYTHONPATH := $(CURDIR);$(CURDIR)/dags;$(PYTHONPATH)

seed:
	$(PYTHON) scripts/seed_data.py

test:
	$(PYTHON) -m pytest -q

lint:
	$(PYTHON) -m ruff check dags checks scripts tests || true

report:
	$(PYTHON) scripts/generate_report.py

pipeline-good: seed
	$(PYTHON) scripts/run_pipeline.py --date 2024-06-01
	$(PYTHON) scripts/generate_report.py

pipeline-bad: seed
	-$(PYTHON) scripts/run_pipeline.py --date 2024-06-03
	$(PYTHON) scripts/generate_report.py

backfill: seed
	@for d in 2024-06-01 2024-06-02 2024-06-03 2024-06-04 2024-06-05; do \
	  $(PYTHON) scripts/run_pipeline.py --date $$d || true; \
	done
	$(PYTHON) scripts/generate_report.py

up:
	docker compose up --build -d

down:
	docker compose down

standalone: seed
	@echo "Lighter fallback: Airflow standalone (needs local install)"
	@mkdir -p $(AIRFLOW_HOME)
	airflow db migrate
	airflow users create --username admin --password admin --firstname Rahul --lastname Ega --role Admin --email rahul@example.com || true
	airflow standalone

ci-local: seed test pipeline-good
	-$(MAKE) pipeline-bad
	$(MAKE) report
