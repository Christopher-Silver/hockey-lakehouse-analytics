# Hockey Lakehouse Analytics Platform

An end-to-end hockey analytics platform built using a lakehouse architecture to transform raw NHL event data into interactive performance insights.

This project demonstrates modern data engineering practices including Medallion architecture, structured pipelines, data quality validation, and BI-ready semantic modeling.

---

## Project Overview

This project explores how a modern data platform can support hockey operations and analytics workflows.

Raw hockey data is ingested from NHL web APIs, processed through a structured pipeline (Bronze → Silver → Gold), and surfaced through Power BI dashboards and ad hoc analysis.

The platform enables insights into:
- goalie performance vs league benchmarks
- player penalty tendencies
- shot selection and accuracy
- team physical play and situational behavior

---

## Architecture

The platform follows a **lakehouse Medallion Architecture**:

- **Bronze** → raw API ingestion (JSON + Delta)
- **Silver** → cleaned, standardized, and validated datasets
- **Gold** → curated fact tables and KPI aggregates
- **BI Layer** → semantic views + Power BI modeling

![Architecture](architecture/lakehouse_architecture.png)

👉 For a detailed breakdown, see:  
**[Pipeline Flow](docs/pipeline_flow.md)**  
**[Data Model Overview](docs/data_model_overview.md)**

---

## Key Features

• Event-level hockey data processing  
• Structured Medallion pipelines (Bronze → Silver → Gold)  
• Curated fact tables and KPI aggregates  
• Hybrid BI layer (Databricks + Power BI)  
• Automated data quality validation  
• Interactive Power BI dashboards  

---

## Interactive Hockey Analytics Dashboards

### Performance Overview

Interactive dashboards provide a high-level view of player and team performance across multiple contexts.

![Dashboard Overview](dashboards/powerbi_overview.png)

---

## Core KPI Dashboards

### 1. Goalie Save % vs League by Shot Type

Compares goalie performance against league averages across shot types.

- Save % breakdown by shot type (wrist, slap, tip-in, etc.)
- Difference vs league benchmarks
- Shot volume context to assess reliability

![Goalie SV%](dashboards/goaliesv.png)

---

### 2. Penalty Profile: Player vs League Tendencies

Analyzes how a player takes and draws penalties relative to league norms.

- Zone-based penalty distribution (Offensive / Defensive / Neutral)
- Game state analysis (leading, tied, trailing)
- Penalty type breakdown vs league averages
- Interactive filtering across situations

![Penalty Profile](dashboards/penaltiesKPI.png)

---

### 3. Goalie Shot-Location Performance

Visualizes where a goalie concedes goals most frequently across the net.

- Spatial goal rate distribution
- Identification of high-risk scoring areas
- Shot volume represented through bin sizing

![Goalie Shot Location](dashboards/kpiGoalieZones.png)

---

### 4. Shot Miss Map

Visualizes where a player misses the net more or less frequently.

- Highlights shooting accuracy trends by location
- Reveals player tendencies (e.g., left/right bias)
- Uses binning and thresholds to reduce noise

![Shot Miss Map](dashboards/Player_Shot_Miss_Map.png)

---

### 5. Hits per 60: Team vs League

Compares team physical play against league benchmarks.

- Hits per 60 by game state (winning, tied, losing)
- Strength state filtering (EV, PP, PK)
- Division-level comparisons

![Hits Profile](dashboards/HitsPer60TiedEvenStrength.png)

---

## Dashboard Deep Dives

Each dashboard includes interactive filtering, contextual comparisons, and insight-driven design.

👉 [View detailed breakdowns](docs/dashboard_breakdowns.md)

---

## Pipelines

This project uses a Medallion lakehouse design in Azure Databricks to transform raw NHL data into analytics-ready outputs.

### Bronze
Raw ingestion from NHL web APIs (schedule and play-by-play).  
Uses a mix of append and merge patterns depending on ingestion grain.

### Silver
Data is cleaned, standardized, and conformed into joinable tables.  
Primarily uses merge-based updates for incremental processing.

### Gold
Curated fact tables and KPI aggregates are built for analytics and reporting.  
Uses overwrite-based rebuilds for consistency and simplicity.

### BI Layer
Databricks provides BI-friendly views, while Power BI handles lightweight semantic modeling (dimensions, measures, relationships).

👉 For full pipeline details:  
**[Pipeline Flow Documentation](docs/pipeline_flow.md)**

---

## Data Model

The data model follows a hybrid dimensional design:

- Core dimensions (player, team) are materialized in Gold
- Fact tables capture event-level data
- KPI tables provide pre-aggregated analytics outputs
- Some semantic dimensions are defined in Power BI

👉 See full model breakdown:  
**[Data Model Overview](docs/data_model_overview.md)**

---

## Technologies Used

Python  
SQL  
Spark / Databricks  
Delta Lake  
Power BI  

---

## Repository Contents

```
architecture/
    Architecture diagrams for the platform

dashboards/
    Power BI dashboard screenshots

pipelines/
    Databricks notebooks and transformation logic

docs/
    Data model, pipeline flow, and design documentation
```

---

## Notes

This repository is a **portfolio case study** of a hockey analytics platform.

It demonstrates:
- end-to-end pipeline design
- structured data modeling
- analytics-ready data preparation
- dashboard-driven insights

Some production elements (e.g., full datasets, orchestration) are simplified or simulated.

---

## Future Improvements

- Automated orchestration using Azure Data Factory  
- Scheduled pipeline execution  
- Expanded feature tables for advanced analytics  
- Player tracking and advanced metrics 
