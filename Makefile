# NYC TLC monthly trip-metrics pipeline — full year 2023 from the NYC Open Data API
# Usage:  conda activate fde
#         make fetch | make run | make test | make notebooks | make all

PY  ?= python
export PYTHONPATH := src

.PHONY: setup fetch ingest validate transform metrics evidence run test notebooks all clean

setup:                      ## create the conda env
	conda env create -f environment.yml

fetch:                      ## retrieve 2023 from the Socrata API (resumable, count-reconciled)
	$(PY) -m nyc_pipeline.api_fetch --out data/reference --raw data/raw --raw-pages data/api_raw

ingest:                     ## fingerprint data/raw into data/manifest.json
	$(PY) -m nyc_pipeline.ingest --raw data/raw --manifest data/manifest.json

validate:                   ## contract rules → staged + quarantine + gate report
	$(PY) -m nyc_pipeline.validate

transform:                  ## staged → trip facts (derived columns + TLC zone ids)
	$(PY) -m nyc_pipeline.transform

metrics:                    ## 5 metrics + KPI + hourly/zone/segment detail
	$(PY) -m nyc_pipeline.metrics --baseline 2023-01

evidence:                   ## render output/evidence_table.md from published artifacts
	$(PY) -m nyc_pipeline.evidence

run:                        ## full pipeline: fetch → ingest → validate → transform → metrics
	$(PY) -m nyc_pipeline.run

test:                       ## unit + sample-based end-to-end tests
	$(PY) -m pytest tests/ -q

notebooks:                  ## execute all evidence notebooks in order
	$(PY) -m jupyter nbconvert --to notebook --execute --inplace \
	    notebooks/01_sources_and_retrieval.ipynb \
	    notebooks/02_profile_and_validation.ipynb \
	    notebooks/03_workflow_and_metrics.ipynb

all: run test evidence      ## pipeline + tests + evidence table

clean:                      ## remove derived outputs (data/raw + data/api_raw untouched)
	rm -rf data/staged/*.parquet data/quarantine/*.parquet
	rm -rf .pytest_cache src/nyc_pipeline/__pycache__ tests/__pycache__
