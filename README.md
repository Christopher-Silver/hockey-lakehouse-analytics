# Hockey Lakehouse Analytics Platform

An end-to-end hockey analytics platform built using a lakehouse architecture to transform raw event data into interactive performance insights.

The system demonstrates modern data engineering practices including medallion architecture, structured data pipelines, and analytics dashboards.

---

## Project Overview

This project was built to explore how modern data platforms can support sports analytics workflows.

Raw hockey event data is ingested and processed through a structured data pipeline before being analyzed through interactive dashboards.

The platform enables insights into player performance, shot patterns, and goalie tendencies.

---

## Architecture

The platform follows a **lakehouse medallion architecture**:

Bronze Layer  
Raw ingested event data.

Silver Layer  
Cleaned and transformed datasets optimized for analysis.

Gold Layer  
Aggregated analytics tables powering dashboards and performance insights.

![Architecture](architecture/lakehouse_architecture.png)

---

## Key Features

• Event-level hockey data processing  
• Structured transformation pipelines  
• Aggregated analytics tables  
• Interactive Power BI dashboards  
• Visual shot maps and performance insights  

---

## Interactive Hockey Analytics Dashboards

### Performance Overview

Interactive dashboards provide a high-level overview of player and team performance metrics.

![Dashboard Overview](dashboards/powerbi_overview.png)

### 🎯 Shot Miss Map

Visualizes where a player misses the net more or less frequently across the offensive zone.

- Identifies accuracy trends by location
- Highlights shooting tendencies (left/right bias, high-danger areas)
- Uses binning + filtering to reduce noise

![Shot Miss Map](dashboards/Player_Shot_Miss_Map.png)

---

### Penalty Profile: Player vs League Tendencies

Compares a player’s penalty behavior against league averages across multiple contexts.

- Zone-based penalty distribution (O/D/N zone)
- Game state analysis (leading, tied, trailing)
- Penalty type breakdown vs league norms
- Fully interactive filtering for situational insights

![Penalty Profile](dashboards/penaltiesKPI.png)

---

### Goalie Shot-Location Performance

Analyzes where a goalie concedes goals most frequently across the net.

- Spatial goal rate distribution
- Identifies high-risk scoring areas
- Shot volume represented through bin sizing

![Goalie Shot Location](dashboards/kpiGoalieZones.png)

---

### Goalie Save % vs League by Shot Type

Breaks down goalie performance by shot type compared to league averages.

- Save % comparison across shot types (wrist, slap, tip-in, etc.)
- Difference vs league to highlight strengths/weaknesses
- Shot volume context included for reliability

![Goalie SV%](dashboards/goaliesv.png)

---

### Hits Profile: Team vs League

Compares team physical play (hits/60) against league benchmarks across contexts.

- Game state breakdown (winning, tied, losing)
- Strength state filtering (EV, PP, PK)
- Division-level comparisons for relative performance

![Hits Profile](dashboards/HitsPer60TiedEvenStrength.png)

---

## Dashboard Deep Dives

Each dashboard includes interactive filtering, contextual comparisons, and insight-driven design.

👉 [View detailed breakdowns](docs/dashboard_breakdowns.md)

...

## Example Data Pipeline

Data transformations convert raw event logs into structured analytics tables.

Example pipeline steps include:

1. Raw event ingestion  
2. Event normalization  
3. Feature engineering  
4. Aggregated performance tables  
5. Dashboard-ready datasets

Example transformation scripts can be found in the `pipelines/` directory.

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
    Screenshots of Power BI dashboards

pipelines/
    Example transformation logic

sql/
    Example analytics queries

docs/
    Data models and design documentation
```

---

## Notes

This repository is a **portfolio case study** of the analytics platform.

Certain production datasets and proprietary assets are not included, but the architecture, representative transformations, and dashboard outputs are demonstrated here.

---

## Future Improvements

Real-time data ingestion  
Automated pipeline orchestration  
Expanded player tracking analytics  
Advanced predictive models
