VENV = venv
PYTHON = $(VENV)/bin/python

.PHONY: init test run-extract transform load all clean

init:
	@echo ">>> Setting up virtual environment..."
	python3 -m venv $(VENV)
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt
	@echo ">>> Environment ready."

test:
	@echo ">>> Running tests..."
	$(PYTHON) -m pytest -q

run-extract:
	@echo ">>> Running extraction..."
	$(PYTHON) -m src.extract.flights --date $(DATE) --limit 100 --max-pages 2

transform:
	@echo ">>> Running PySpark transformation..."
	$(PYTHON) -m src.transform.transform_flights

load:
	@echo ">>> Uploading processed data to S3..."
	$(PYTHON) -m src.load.upload_to_s3

all:
	make run-extract DATE=$(DATE)
	make transform
	make load

clean:
	@echo ">>> Cleaning up..."
	rm -rf data/raw/* data/processed/* logs/*
