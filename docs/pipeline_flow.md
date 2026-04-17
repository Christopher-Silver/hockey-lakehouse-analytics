# Pipeline Flow

## Overview

This project implements an end-to-end data pipeline using a Medallion Architecture:

Bronze → Silver → Gold

Data is sourced from NHL web APIs, processed in Azure Databricks, and stored in Delta Lake tables within ADLS Gen2. The final outputs are consumed by Power BI dashboards and analysis workflows.

---

## Data Sources

Data is ingested from NHL web APIs, including:

- schedule endpoints
- play-by-play endpoints

These APIs provide raw game and event data used throughout the pipeline.

---

## Bronze Layer (Raw Ingestion)

The Bronze layer stores raw data with minimal transformation.

Two ingestion patterns are used:

- **Append-based ingestion**  
  Used for schedule and general raw data landing

- **Merge-based ingestion**  
  Used for play-by-play data to prevent duplicate game payloads during reruns

Purpose:
- preserve raw data
- ensure reproducibility
- support safe reprocessing

Raw data exists in both JSON form and Bronze Delta tables.

---

## Silver Layer (Cleaning & Conformance)

The Silver layer standardizes and validates the data.

Key processes:
- parsing raw API payloads into structured columns
- normalizing identifiers (`player_id`, `team_id`, `game_id`)
- deduplication using natural keys (e.g., `game_id + event_id`)
- handling missing or inconsistent values

Write pattern:
- primarily **merge-based**, allowing incremental updates and rerun safety

Key outputs:
- `silver.events`
- `silver.players`
- `silver.player_game_roster`

Purpose:
- produce clean, joinable datasets
- enforce data quality and consistency

---

## Gold Layer (Analytics & KPI Modeling)

The Gold layer produces analytics-ready datasets.

### Fact Tables

- `fact_shot_attempt`
- `fact_penalty`
- `fact_hit`

These are curated, narrow schemas derived from Silver events.

---

### KPI Tables

Pre-aggregated tables support reporting and analysis:

- goalie performance grids
- shot miss maps
- penalty metrics
- hits per 60 by strength state

---

### Write Strategy

- Fact tables: **overwrite**
- KPI tables: **overwrite**

This is intentional because:
- Gold is derived from curated upstream data
- full rebuilds are simpler and more reliable than incremental merges

---

## BI Serving Layer (Databricks)

A set of BI-friendly views is created in Databricks:

- `gold_bi.v_*`

These views:
- rename fields for clarity
- add readable labels (e.g., zone names)
- parse strength state from `situation_code`

Write pattern:
- `CREATE OR REPLACE VIEW`

Purpose:
- simplify Power BI integration
- avoid duplicating physical tables

---

## Data Quality & Validation

Data quality checks are implemented in the Gold layer.

Checks include:
- primary key uniqueness
- null validation for critical fields
- domain validation (e.g., zone codes)
- coordinate bounds checks

Results are written to:

- `dq_results`
- `dq_failures`

Write pattern:
- **append** (to maintain run history)

Purpose:
- monitor pipeline health
- identify data issues early
- support debugging and auditing

---

## Data Consumption

Data is consumed through a hybrid BI layer:

### Databricks
- Gold tables
- BI views (`gold_bi`)

### Power BI
- dashboards
- measures and calculations
- lightweight dimensions (e.g., situation, zone, score state)

This hybrid approach allows:
- strong backend consistency
- flexible front-end reporting

---

## Future Enhancements

Planned improvements include:

- orchestration using Azure Data Factory for scheduled ingestion
- automated pipeline execution and monitoring
- expanded feature tables for advanced analytics

---

## Summary

The pipeline is designed to:

- ingest and store raw hockey data reliably
- standardize and validate data for analysis
- produce fast, analytics-ready datasets
- support both structured dashboards and ad hoc exploration

Each layer has a clear responsibility, and write patterns are chosen based on the purpose of the data at that stage.
