# WanderBricks — Databricks Medallion Pipeline

A medallion architecture data pipeline built on Databricks, demonstrating 
enterprise-grade streaming ETL patterns using Delta Lake, Lakeflow Jobs, and 
parameterized notebooks.

## Overview

This project ingests travel booking data from the `samples.wanderbricks` 
Delta Share dataset and processes it through Bronze, Silver, and Gold layers 
using a three-task Lakeflow Job on serverless compute, implementing a fully 
incremental pipeline with streaming ingestion and incremental Gold layer 
aggregation.

## Architecture

```
Delta Share (Source)
       ↓
  Bronze Layer       — Streaming ingestion, raw schema enforcement
       ↓
  Silver Layer       — Cleansed, deduplicated, idempotent merge
       ↓
  Gold Layer         — Incremental aggregation, analytics-ready
```

## Pipeline Design

- **Orchestration:** Lakeflow Jobs (3-task DAG)
- **Compute:** Serverless
- **Storage:** Delta Lake with Unity Catalog
- **Write Pattern:** Idempotent Delta merge (insert/update)
- **Processing Pattern:** Streaming ingestion at Bronze, incremental merge at Silver and Gold for a fully incremental end-to-end pipeline
- **Parameters:** Metadata-driven, notebook-parameterized

## Repository Structure

```
databricks/
├── databricks.yml              # DAB bundle config (reference implementation)
├── resources/
│   └── wanderbricks_job.yml    # Job definition
├── src/
│   ├── WBrSetup.py             # Environment reset utility
│   ├── WBrETL1Bronze.py        # Bronze ingestion notebook
│   ├── WBrETL2Silver.py        # Silver transformation notebook
│   └── WBrETL3Gold.py          # Gold aggregation notebook
└── utils/
    └── Scratchpad.py           # Dev utilities, not part of pipeline
```

## Next Steps

- DAB deployment: Bundle is structured as a Declarative Automation Bundle
reference implementation. Full deployment requires a non-Community Edition
workspace with Git integration enabled.

- Dev/Prod targets: Add environment-specific cluster configurations via
DAB target overrides.

- Control table: Migrate pipeline parameters from notebook-level to a
Unity Catalog control table for metadata-driven orchestration at scale.

- CI/CD: Add GitHub Actions workflow to automate validate and deploy on
push to main.